'''
Build a spatial-temporal attention-based video caption generator
'''
import theano
import theano.tensor as tensor
import theano.tensor as T
from theano.sandbox.rng_mrg import MRG_RandomStreams as RandomStreams

import cPickle as pkl
import numpy
import copy
import os, sys, socket, shutil
import time
import warnings
from collections import OrderedDict

from sklearn.cross_validation import KFold
from scipy import optimize, stats

import data_engine
import metrics
import common


from common import *

base_path = None
hostname = socket.gethostname()
lscratch_dir = None

# make prefix-appended name
def _p(pp, name):
    return '%s_%s'%(pp, name)

def validate_options(options):
    if options['ctx2out']:
        warnings.warn('Feeding context to output directly seems to hurt.')
    if options['dim_word'] > options['dim']:
        warnings.warn('dim_word should only be as large as dim.')
    return options

class Attention(object):
    def __init__(self, channel=None):
        # layers: 'name': ('parameter initializer', 'feedforward')
        self.layers = {
            'ff': ('self.param_init_fflayer', 'self.fflayer'),
            'lstm': ('self.param_init_lstm', 'self.lstm_layer'),
            'lstm_cond': ('self.param_init_lstm_cond', 'self.lstm_cond_layer'),
            }
        self.channel = channel

    def get_layer(self, name):
        """
        Part of the reason the init is very slow is because,
        the layer's constructor is called even when it isn't needed
        """
        fns = self.layers[name]
        return (eval(fns[0]), eval(fns[1]))

    def load_params(self, path, params):
        # load params from disk
        pp = numpy.load(path)
        for kk, vv in params.iteritems():
            if kk not in pp:
                raise Warning('%s is not in the archive'%kk)
            params[kk] = pp[kk]

        return params

    def init_tparams(self, params, force_cpu=False):
        # initialize Theano shared variables according to the initial parameters
        tparams = OrderedDict()
        for kk, pp in params.iteritems():
            if force_cpu:
                tparams[kk] = theano.tensor._shared(params[kk], name=kk)
            else:
                tparams[kk] = theano.shared(params[kk], name=kk)
        return tparams

    def param_init_fflayer(self, options, params, prefix='ff', nin=None, nout=None):
        if nin == None:
            nin = options['dim_proj']
        if nout == None:
            nout = options['dim_proj']
        params[_p(prefix,'W')] = norm_weight(nin, nout, scale=0.01)
        params[_p(prefix,'b')] = numpy.zeros((nout,)).astype('float32')
        return params

    def fflayer(self, tparams, state_below, options,
                prefix='rconv', activ='lambda x: tensor.tanh(x)', **kwargs):
        return eval(activ)(tensor.dot(state_below, tparams[_p(prefix,'W')])+tparams[
            _p(prefix,'b')])

    # LSTM layer
    def param_init_lstm(self, options, params, prefix=None, nin=None, dim=None):
        assert prefix is not None
        if nin == None:
            nin = options['dim_proj']
        if dim == None:
            dim = options['dim_proj']
        # Stack the weight matricies for faster dot prods
        W = numpy.concatenate([norm_weight(nin,dim),
                               norm_weight(nin,dim),
                               norm_weight(nin,dim),
                               norm_weight(nin,dim)], axis=1)
        params[_p(prefix,'W')] = W
        U = numpy.concatenate([ortho_weight(dim),
                               ortho_weight(dim),
                               ortho_weight(dim),
                               ortho_weight(dim)], axis=1)
        params[_p(prefix,'U')] = U
        params[_p(prefix,'b')] = numpy.zeros((4 * dim,)).astype('float32')

        return params

    # This function implements the lstm fprop
    def lstm_layer(self, tparams, state_below, options, prefix='lstm', mask=None,
                   forget=False, use_noise=None, trng=None, **kwargs):
        nsteps = state_below.shape[0]
        dim = tparams[_p(prefix,'U')].shape[0]

        if state_below.ndim == 3:
            n_samples = state_below.shape[1]
            init_state = tensor.alloc(0., n_samples, dim)
            init_memory = tensor.alloc(0., n_samples, dim)
        else:
            n_samples = 1
            init_state = tensor.alloc(0., dim)
            init_memory = tensor.alloc(0., dim)

        if mask == None:
            mask = tensor.alloc(1., state_below.shape[0], 1)

        def _slice(_x, n, dim):
            if _x.ndim == 3:
                return _x[:, :, n*dim:(n+1)*dim]
            elif _x.ndim == 2:
                return _x[:, n*dim:(n+1)*dim]
            return _x[n*dim:(n+1)*dim]

        def _step(m_, x_, h_, c_, U, b):
            preact = tensor.dot(h_, U)
            preact += x_
            preact += b

            i = tensor.nnet.sigmoid(_slice(preact, 0, dim))
            f = tensor.nnet.sigmoid(_slice(preact, 1, dim))
            o = tensor.nnet.sigmoid(_slice(preact, 2, dim))
            c = tensor.tanh(_slice(preact, 3, dim))

            if forget:
                f = T.zeros_like(f)
            c = f * c_ + i * c
            h = o * tensor.tanh(c)
            if m_.ndim == 0:
                # when using this for minibatchsize=1
                h = m_ * h + (1. - m_) * h_
                c = m_ * c + (1. - m_) * c_
            else:
                h = m_[:,None] * h + (1. - m_)[:,None] * h_
                c = m_[:,None] * c + (1. - m_)[:,None] * c_
            return h, c, i, f, o, preact

        state_below = tensor.dot(
            state_below, tparams[_p(prefix, 'W')]) + tparams[_p(prefix, 'b')]
        U = tparams[_p(prefix, 'U')]
        b = tparams[_p(prefix, 'b')]
        rval, updates = theano.scan(
            _step,
            sequences=[mask, state_below],
            non_sequences=[U,b],
            outputs_info = [init_state, init_memory, None, None, None, None],
            name=_p(prefix, '_layers'),
            n_steps=nsteps,
            strict=True,
            profile=False)
        return rval

    # Conditional LSTM layer with Attention
    def param_init_lstm_cond(self, options, params,
                             prefix='lstm_cond', nin=None, dim=None, dimctxglm=None,dimctxg=None,dimctxl=None,dimctxm=None):
        if nin == None:
            nin = options['dim']
        if dim == None:
            dim = options['dim']
        if dimctxglm == None:
            dimctxglm = options['dim']
        # input to LSTM
        W = numpy.concatenate([norm_weight(nin,dim),
                               norm_weight(nin,dim),
                               norm_weight(nin,dim),
                               norm_weight(nin,dim)], axis=1)
        params[_p(prefix,'W')] = W

        # LSTM to LSTM
        U = numpy.concatenate([ortho_weight(dim),
                               ortho_weight(dim),
                               ortho_weight(dim),
                               ortho_weight(dim)], axis=1)
        params[_p(prefix,'U')] = U

        # bias to LSTM
        params[_p(prefix,'b')] = numpy.zeros((4 * dim,)).astype('float32')

        # context_global+motion+local to LSTM
        Wc = norm_weight(dimctxglm,dim*4)
        params[_p(prefix,'Wc')] = Wc

        #temporal attention: context_global -> hidden
        Wcg_att = norm_weight(dimctxg, ortho=False)
        params[_p(prefix,'Wcg_att')] = Wcg_att
		
        # context_motion -> hidden
        Wcm_att = norm_weight(dimctxm, ortho= False)
        params[_p(prefix, 'Wcm_att')] = Wcm_att
		
        #context_local ->hidden
        Wclt_att = norm_weight(dimctxl, ortho=False)
        params[_p(prefix, 'Wclt_att')] = Wclt_att

        #temporal attention: LSTM -> hidden
        Wdg_att = norm_weight(dim,dimctxg)
        params[_p(prefix,'Wdg_att')] = Wdg_att
		
        Wdm_att = norm_weight(dim,dimctxm)
        params[_p(prefix,'Wdm_att')] = Wdm_att

        Wdlt_att = norm_weight(dim,dimctxl)
        params[_p(prefix,'Wdlt_att')] = Wdlt_att

        #temporal attention: hidden bias
        bg_att = numpy.zeros((dimctxg,)).astype('float32')
        params[_p(prefix,'bg_att')] = bg_att
		
        bm_att = numpy.zeros((dimctxm,)).astype('float32')
        params[_p(prefix, 'bm_att')] = bm_att

        
        blt_att = numpy.zeros((dimctxl,)).astype('float32')
        params[_p(prefix, 'blt_att')] = blt_att

        #spatial attention: context_local->hidden
        Wcl_att = norm_weight(dimctxl, ortho=False)
        params[_p(prefix,'Wcl_att')] = Wcl_att

        #spatial attention:LSTM->hidden
        Wdl_att = norm_weight(dim,dimctxl)
        params[_p(prefix,'Wdl_att')] = Wdl_att

        #spatial attention: hidden bias
        bl_att = numpy.zeros((dimctxl,)).astype('float32')
        params[_p(prefix,'bl_att')] = bl_att

        #time attention:
        Ug_att = norm_weight(dimctxg,1)
        params[_p(prefix,'Ug_att')] = Ug_att
        cg_att = numpy.zeros((1,)).astype('float32')
        params[_p(prefix, 'cg_att')] = cg_att
		
        Um_att = norm_weight(dimctxm,1)
        params[_p(prefix,'Um_att')] = Um_att
        cm_att = numpy.zeros((1,)).astype('float32')
        params[_p(prefix, 'cm_att')] = cm_att

        Ult_att = norm_weight(dimctxl,1)
        params[_p(prefix,'Ult_att')] = Ult_att
        clt_att = numpy.zeros((1,)).astype('float32')
        params[_p(prefix, 'clt_att')] = clt_att

        #sptial attention:
        Ul_att = norm_weight(dimctxl,1)
        params[_p(prefix,'Ul_att')] = Ul_att
        cl_att = numpy.zeros((1,)).astype('float32')
        params[_p(prefix, 'cl_att')] = cl_att

        if options['selector']:
            # attention: selector
            W_sel = norm_weight(dim, 1)
            params[_p(prefix, 'W_sel')] = W_sel
            b_sel = numpy.float32(0.)
            params[_p(prefix, 'b_sel')] = b_sel
        return params

    def lstm_cond_layer(self, tparams, state_below, options, prefix='lstm',
                        mask=None, context_global=None, context_local=None, context_motion=None, one_step=False,
                        init_memory=None, init_state=None,
                        trng=None, use_noise=None,mode=None,
                        **kwargs):
        # state_below (t, m, dim_word), or (m, dim_word) in sampling
        # mask (t, m)
        # context1 (m, f, dim_ctx1), or (f, dim_ctx1) in sampling
        # context2(m,f,n,dim_ctx2), or (f,n,dim_ctx2) in sampling
        # init_memory, init_state (m, dim)
        assert context_global, 'Context_global must be provided'
        assert context_local, 'Context_local must be provided'
        assert context_motion, 'Context_motion must be provided'


        if one_step:
            assert init_memory, 'previous memory must be provided'
            assert init_state, 'previous state must be provided'

        nsteps = state_below.shape[0]
        if state_below.ndim == 3:
            n_samples = state_below.shape[1]
        else:
            n_samples = 1

        # mask
        if mask == None:
            mask = tensor.alloc(1., state_below.shape[0], 1)

        dim = tparams[_p(prefix, 'U')].shape[0]

        # initial/previous state
        if init_state == None:
            init_state = tensor.alloc(0., n_samples, dim)
        # initial/previous memory
        if init_memory == None:
            init_memory = tensor.alloc(0., n_samples, dim)
        #projected context_global
        pctxg_ = tensor.dot(context_global, tparams[_p(prefix, 'Wcg_att')]) + tparams[_p(prefix, 'bg_att')]
        # projected context_local
        pctxl_ = tensor.dot(context_local,tparams[_p(prefix,'Wcl_att')]) + tparams[_p(prefix, 'bl_att')]
        #projected context_motion
        pctxm_ = tensor.dot(context_motion,tparams[_p(prefix,'Wcm_att')]) + tparams[_p(prefix, 'bm_att')]

        if one_step:
            # tensor.dot will remove broadcasting dim
            pctxg_ = T.addbroadcast(pctxg_, 0)
            pctxl_ = T.addbroadcast(pctxl_,0)
            pctxm_ = T.addbroadcast(pctxm_,0)
        # projected x
        state_below = tensor.dot(state_below, tparams[_p(prefix, 'W')]) + tparams[
            _p(prefix, 'b')]

        Wdg_att = tparams[_p(prefix,'Wdg_att')]
        Wdl_att = tparams[_p(prefix,'Wdl_att')]
        Wdm_att = tparams[_p(prefix,'Wdm_att')]
        Wdlt_att = tparams[_p(prefix, 'Wdlt_att')]
		
        Ug_att = tparams[_p(prefix,'Ug_att')]
        cg_att = tparams[_p(prefix, 'cg_att')]
        Ul_att = tparams[_p(prefix, 'Ul_att')]
        cl_att = tparams[_p(prefix, 'cl_att')]
        Um_att = tparams[_p(prefix, 'Um_att')]
        cm_att = tparams[_p(prefix, 'cm_att')]
        Ult_att = tparams[_p(prefix, 'Ult_att')]
        clt_att = tparams[_p(prefix,'clt_att')]
		
        Wclt_att = tparams[_p(prefix, 'Wclt_att')]
        blt_att = tparams[_p(prefix, 'blt_att')]
        if options['selector']:
            W_sel = tparams[_p(prefix, 'W_sel')]
            b_sel = tparams[_p(prefix,'b_sel')]
        else:
            W_sel = T.alloc(0., 1)
            b_sel = T.alloc(0., 1)
        U = tparams[_p(prefix, 'U')]
        Wc = tparams[_p(prefix, 'Wc')]
        def _slice(_x, n, dim):
            if _x.ndim == 3:
                return _x[:, :, n*dim:(n+1)*dim]
            return _x[:, n*dim:(n+1)*dim]

        def _step(m_, x_, # sequences
                  h_, c_,al_,ctl_,ag_,ctg_,am_, ctm_, alt_,ctlt_,ctgltm_, # outputs_info
                  pctxl_,pctxg_, pctxm_, ctxl_,ctxg_,ctxm_,Wdl_att,Ul_att,cl_att,Wdg_att,Ug_att,cg_att,Wdm_att, Um_att,cm_att,Wdlt_att,Wclt_att,blt_att,Ult_att,clt_att,W_sel,b_sel, U, Wc, # non_sequences
                  dp_=None, dp_att_=None):
            #spatial attention
            pstatel_ = tensor.dot(h_, Wdl_att)
            pctxl_ = pctxl_ + pstatel_[:,:,None,:]
            pctxl_list = []
            pctxl_list.append(pctxl_)
            pctxl_ = tanh(pctxl_)
            
            alphal = tensor.dot(pctxl_, Ul_att)+cl_att
            alphal_pre = alphal
            alphal_shp = alphal.shape
            alphal = tensor.nnet.softmax(alphal.reshape([alphal_shp[0]*alphal_shp[1],alphal_shp[2]])) # softmax
            alphal = alphal.reshape([alphal_shp[0],alphal_shp[1],alphal_shp[2]])

            ctxl_ = (context_local * alphal[:,:,:,None]).sum(2) # (m,f,ctxl_dim)
            #context = T.concatenate((pctx1_,ctx2_),axis = 2)
            # if one_step:
            #     ctx2_ = ctx2_.dimshuffle('x',0,1,2)

            #temporal attention,context_global
            pstateg_ = tensor.dot(h_, Wdg_att)
            pctxg_ = pctxg_+pstateg_[:,None,:]
            pctxg_list = []
            pctxg_list.append(pctxg_)
            pctxg_ = tanh(pctxg_)

            alphag = tensor.dot(pctxg_, Ug_att)+cg_att
            alphag_pre = alphag
            alphag_shp = alphag.shape
            alphag = tensor.nnet.softmax(alphag.reshape([alphag_shp[0],alphag_shp[1]]))#softmax
            ctxg_ = (context_global * alphag[:,:,None]).sum(1) # (m,ctxg_dim)
			
            #temporal attention, context_motion
            pstatem_ = tensor.dot(h_, Wdm_att)
            pctxm_ = pctxm_+pstatem_[:,None,:]
            pctxm_list = []
            pctxm_list.append(pctxm_)
            pctxm_ = tanh(pctxm_)

            alpham = tensor.dot(pctxm_, Um_att)+cm_att
            alpham_pre = alpham
            alpham_shp = alpham.shape
            alpham = tensor.nnet.softmax(alpham.reshape([alpham_shp[0],alpham_shp[1]]))#softmax
            ctxm_ = (context_motion * alpham[:,:,None]).sum(1) # (m,ctxm_dim)

            #temporal attention,ctxl_
            pstatelt_ = tensor.dot(h_, Wdlt_att)
            pctxlt_ = tensor.dot(ctxl_,tparams[_p(prefix,'Wclt_att')]) + tparams[_p(prefix, 'blt_att')]
            pctxlt_ = pctxlt_ + pstatelt_[:,None,:]
            pctxlt_list = []
            pctxlt_list.append(pctxlt_)
            pctxlt_ = tanh(pctxlt_)

            alphalt = tensor.dot(pctxlt_, Ult_att) + clt_att
            alphalt_pre = alphalt
            alphalt_shp = alphalt.shape
            alphalt = tensor.nnet.softmax(alphalt.reshape([alphalt_shp[0], alphalt_shp[1]]))  # softmax
            ctxlt_ = (ctxl_ * alphalt[:,:,None]).sum(1)  # (m,ctxl_dim)

            #fusion of global, local and motion features
            #ctxgltm_ = T.concatenate([ctxg_,ctxm_, ctxlt_],axis=1)
            ctxgltm_=ctxg_+ctxm_+ctxlt_

            if options['selector']:
                sel_ = tensor.nnet.sigmoid(tensor.dot(h_, W_sel) + b_sel)
                sel_ = sel_.reshape([sel_.shape[0]])
                ctxgltm_ = sel_[:,None] * ctxgltm_

            preact = tensor.dot(h_, U)
            preact += x_
            preact += tensor.dot(ctxgltm_, Wc)

            i = _slice(preact, 0, dim)
            f = _slice(preact, 1, dim)
            o = _slice(preact, 2, dim)
            if options['use_dropout']:
                i = i * _slice(dp_, 0, dim)
                f = f * _slice(dp_, 1, dim)
                o = o * _slice(dp_, 2, dim)
            i = tensor.nnet.sigmoid(i)
            f = tensor.nnet.sigmoid(f)
            o = tensor.nnet.sigmoid(o)
            c = tensor.tanh(_slice(preact, 3, dim))

            c = f * c_ + i * c
            c = m_[:,None] * c + (1. - m_)[:,None] * c_

            h = o * tensor.tanh(c)
            h = m_[:,None] * h + (1. - m_)[:,None] * h_
            rval = [h, c, alphal, ctxl_, alphag, ctxg_,  alpham,ctxm_, alphalt, ctxlt_, ctxgltm_, pstatel_, pstateg_,pstatem_, pstatelt_,pctxl_, pctxg_, pctxm_, pctxlt_,i, f, o, preact, alphal_pre, alphag_pre, alpham_pre, alphalt_pre]+pctxl_list+pctxg_list+pctxm_list+pctxlt_list
            return rval
        if options['use_dropout']:
            _step0 = lambda m_, x_, dp_, h_, c_, \
                al_, ctl_, ag_, ctg_,am_,ctm_,alt_,ctlt_,ctgltm_,\
                pctxl_, pctxg_,pctxm_,context_local, context_global,context_motion, Wdl_att,Ul_att,cl_att,Wdg_att,Ug_att,cg_att,Wdm_att, Um_att,cm_att,Wdlt_att,Wclt_att,blt_att,Ult_att,clt_att,W_sel,b_sel, U, Wc,: _step(
                m_, x_, h_, c_,
                al_, ctl_, ag_, ctg_, am_,ctm_, alt_,ctlt_,ctgltm_,\
                pctxl_, pctxg_,pctxm_, context_local, context_global, context_motion,  Wdl_att,Ul_att,cl_att,Wdg_att,Ug_att,cg_att,Wdm_att, Um_att, cm_att, Wdlt_att,Wclt_att,blt_att,Ult_att,clt_att,W_sel,b_sel, U, Wc,  dp_)
            dp_shape = state_below.shape
            if one_step:
                dp_mask = tensor.switch(use_noise,
                                        trng.binomial((dp_shape[0], 3*dim),
                                                      p=0.5, n=1, dtype=state_below.dtype),
                                        tensor.alloc(0.5, dp_shape[0], 3 * dim))
            else:
                dp_mask = tensor.switch(use_noise,
                                        trng.binomial((dp_shape[0], dp_shape[1], 3*dim),
                                                      p=0.5, n=1, dtype=state_below.dtype),
                                        tensor.alloc(0.5, dp_shape[0], dp_shape[1], 3*dim))
        else:
            _step0 = lambda m_, x_, h_, c_,a_, ct_, pctx_, context, Wd_att, U_att, c_att, W_sel, b_sel, U, Wc: _step(
                m_, x_, h_, c_, a_, ct_, pctx_, context,
                    Wd_att, U_att, c_att, W_sel, b_sel, U, Wc)

        if one_step:
            if options['use_dropout']:
                rval = _step0(
                    mask, state_below, dp_mask, init_state, init_memory, None, None,None,None,None,None,None, None,None,
                     pctxl_, pctxg_,pctxm_, context_local, context_global,context_motion, Wdl_att,Ul_att,cl_att,Wdg_att,Ug_att,cg_att,Wdm_att, Um_att, cm_att,Wdlt_att,Wclt_att,blt_att,Ult_att,clt_att,W_sel,b_sel, U, Wc)
            else:
                rval = _step0(mask, state_below, init_state, init_memory, None, None,
                              pctx_, context, Wd_att, U_att, c_att, W_sel, b_sel, U, Wc)
        else:
            seqs = [mask, state_below]
            if options['use_dropout']:
                seqs += [dp_mask]
            rval, updates = theano.scan(
                _step0,
                sequences=seqs,
                outputs_info = [init_state,
                                init_memory,
                                tensor.alloc(0., n_samples, pctxl_.shape[1], pctxl_.shape[2]),#al
                                tensor.alloc(0., n_samples, context_local.shape[1], context_local.shape[3]),#ctl_
                                tensor.alloc(0., n_samples, pctxg_.shape[1]),#ag
                                tensor.alloc(0., n_samples, context_global.shape[2]),#ctg_
								tensor.alloc(0., n_samples, pctxm_.shape[1]),#am
                                tensor.alloc(0., n_samples, context_motion.shape[2]),#ctm_
                                tensor.alloc(0., n_samples, pctxl_.shape[1]),#a3
                                tensor.alloc(0., n_samples, context_local.shape[3]),#ct3_
                                tensor.alloc(0., n_samples, Wc.shape[0]),#ctgltm_
                                None, None, None, None, None, None, None, None, None, None, None, None, None, None,None,None, None, None, None,None],
                                non_sequences=[pctxl_, pctxg_,pctxm_, context_local, context_global, context_motion, Wdl_att,Ul_att,cl_att,Wdg_att,Ug_att,cg_att,Wdm_att, Um_att, cm_att, Wdlt_att,Wclt_att,blt_att,Ult_att,clt_att, W_sel, b_sel, U, Wc],
                                name=_p(prefix, '_layers'),
                                n_steps=nsteps, profile=False, mode=mode, strict=True)

        return rval
    """---------------------------------------------------------------------------------"""
    """---------------------------------------------------------------------------------"""
    """---------------------------------------------------------------------------------"""
    def init_params(self, options):
        # all parameters
        params = OrderedDict()
        # embedding
        params['Wemb'] = norm_weight(options['n_words'], options['dim_word'])

        if options['encoder'] == 'lstm_bi':
            print 'bi-directional lstm encoder on ctx'
            params = self.get_layer('lstm')[0](options, params, prefix='encoder',
                                          nin=options['ctx_dim'], dim=options['encoder_dim'])
            params = self.get_layer('lstm')[0](options, params, prefix='encoder_rev',
                                          nin=options['ctx_dim'], dim=options['encoder_dim'])
            ctx_dim = options['encoder_dim'] * 2 + options['ctx_dim']

        elif options['encoder'] == 'lstm_uni':
            print 'uni-directional lstm encoder on ctx'
            params = self.get_layer('lstm')[0](options, params, prefix='encoder',
                                          nin=options['ctx_dim'], dim=options['dim'])
            ctx_dim = options['dim']

        else:
            print 'no lstm on ctx'
            ctxglm_dim = options['ctxglm_dim']
            ctxg_dim = options['ctxg_dim']
            ctxl_dim = options['ctxl_dim']
            ctxm_dim = options['ctxm_dim']

        # init_state, init_cell
        for lidx in xrange(options['n_layers_init']):
            params = self.get_layer('ff')[0](
                options, params, prefix='ff_init_%d'%lidx, nin=ctx_dim, nout=ctx_dim)
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_state', nin=ctxg_dim, nout=options['dim'])
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_memory', nin=ctxg_dim, nout=options['dim'])
        # params = self.get_layer('ff')[0](
        #     options, params, prefix='ff_global', nin=ctxg_dim, nout=options['dim'])
		
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_local', nin=ctxl_dim, nout=options['dim'])
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_motion', nin=ctxm_dim, nout=options['dim'])
        # decoder: LSTM
        params = self.get_layer('lstm_cond')[0](options, params, prefix='decoder',
                                           nin=options['dim_word'], dim=options['dim'],
                                           dimctxg=options['dim'], dimctxl=options['dim'],dimctxm=options['dim'],dimctxglm=options['dim'])
        
        # readout
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_logit_lstm',
            nin=options['dim'], nout=options['dim_word'])
        if options['ctx2out']:
            params = self.get_layer('ff')[0](
                options, params, prefix='ff_logit_ctxglm',
                nin=ctxglm_dim, nout=options['dim_word'])
        if options['n_layers_out'] > 1:
            for lidx in xrange(1, options['n_layers_out']):
                params = self.get_layer('ff')[0](
                    options, params, prefix='ff_logit_h%d'%lidx,
                    nin=options['dim_word'], nout=options['dim_word'])
        params = self.get_layer('ff')[0](
            options, params, prefix='ff_logit',
            nin=options['dim_word'], nout=options['n_words'])
        return params

    def build_model(self, tparams, options):
        trng = RandomStreams(1234)
        use_noise = theano.shared(numpy.float32(0.))
        # description string: #words x #samples
        x = tensor.matrix('x', dtype='int64')
        x.tag.test_value = self.x_tv
        mask = tensor.matrix('mask', dtype='float32')
        mask.tag.test_value = self.mask_tv
        # context_global: #samples x #annotations x dim
        ctxg = tensor.tensor3('ctxg', dtype='float32')
        ctxg.tag.test_value = self.ctxg_tv
        mask_ctxg = tensor.matrix('mask_ctxg', dtype='float32')
        mask_ctxg.tag.test_value = self.ctxg_mask_tv
        
        # context_local:(m,f,n,ctxl_dim)
        ctxl = tensor.tensor4('ctxl', dtype='float32')
        ctxl.tag.test_value = self.ctxl_tv
        mask_ctxl = tensor.tensor3('mask_ctxl', dtype='float32')
        mask_ctxl.tag.test_value = self.ctxl_mask_tv
		
        #context_motion
        ctxm = tensor.tensor3('ctxm', dtype='float32')
        ctxm.tag.test_value = self.ctxm_tv
        mask_ctxm = tensor.matrix('mask_ctxm', dtype='float32')
        mask_ctxm.tag.test_value = self.ctxm_mask_tv

        n_timesteps = x.shape[0]
        n_samples = x.shape[1]

        # index into the word embedding matrix, shift it forward in time
        emb = tparams['Wemb'][x.flatten()].reshape(
            [n_timesteps, n_samples, options['dim_word']])
        emb_shifted = tensor.zeros_like(emb)
        emb_shifted = tensor.set_subtensor(emb_shifted[1:], emb[:-1])
        emb = emb_shifted
        counts = mask_ctxg.sum(-1).dimshuffle(0,'x')

        ctxg_ = ctxg
        ctxl_ = ctxl
        ctxm_ = ctxm

        if options['encoder'] == 'lstm_bi':
            # encoder
            ctx_fwd = self.get_layer('lstm')[1](tparams, ctx_.dimshuffle(1,0,2),
                                           options, mask=mask_ctx.dimshuffle(1,0),
                                           prefix='encoder')[0]
            ctx_rev = self.get_layer('lstm')[1](tparams, ctx_.dimshuffle(1,0,2)[::-1],
                                                options, mask=mask_ctx.dimshuffle(1,0)[::-1],
                                                prefix='encoder_rev')[0]
            ctx0 = concatenate((ctx_fwd, ctx_rev[::-1]), axis=2)
            ctx0 = ctx0.dimshuffle(1,0,2)
            ctx0 = concatenate((ctx_, ctx0), axis=2)
            ctx_mean = ctx0.sum(1)/counts

        elif options['encoder'] == 'lstm_uni':
            ctx0 = self.get_layer('lstm')[1](tparams, ctx_.dimshuffle(1,0,2),
                                           options,
                                           mask=mask_ctx.dimshuffle(1,0),
                                           prefix='encoder')[0]
            ctx0 = ctx0.dimshuffle(1,0,2)
            ctx_mean = ctx0.sum(1)/counts

        else:
            ctxg_0 = ctxg_
            ctxl_0 = ctxl_
            ctxm_0 = ctxm_
            ctxg_mean = ctxg_0.sum(1)/counts
        # initial state/cell
        for lidx in xrange(options['n_layers_init']):
            ctx_mean = self.get_layer('ff')[1](
                tparams, ctx1_mean, options, prefix='ff_init_%d'%lidx, activ='rectifier')
            if options['use_dropout']:
                ctx_mean = dropout_layer(ctx1_mean, use_noise, trng)

        init_state = self.get_layer('ff')[1](
            tparams, ctxg_mean, options, prefix='ff_state', activ='tanh')
        init_memory = self.get_layer('ff')[1](
            tparams, ctxg_mean, options, prefix='ff_memory', activ='tanh')
        # ctxg_0 = self.get_layer('ff')[1](
        #     tparams, ctxg_0, options, prefix='ff_global', activ='tanh')
		# adding a non_linear transformation layer with motion and local features to keep consistent with LSTM dimension
        ctxl_0=self.get_layer('ff')[1](
            tparams, ctxl_0, options, prefix='ff_local', activ='tanh')
        ctxm_0 = self.get_layer('ff')[1](
            tparams, ctxm_0, options, prefix='ff_motion', activ='tanh')
        # decoder
        proj = self.get_layer('lstm_cond')[1](tparams, emb, options,
                                         prefix='decoder',
                                         mask=mask, context_global=ctxg_0,context_local=ctxl_0,context_motion=ctxm_0,
                                         one_step=False,
                                         init_state=init_state,
                                         init_memory=init_memory,
                                         trng=trng,
                                         use_noise=use_noise)

        proj_h = proj[0]
        alphals = proj[2]
        alphags = proj[4]
        alphams = proj[6]
        alphalts = proj[8]
        ctxgltms = proj[10]
        if options['use_dropout']:
            proj_h = dropout_layer(proj_h, use_noise, trng)
        # compute word probabilities
        logit = self.get_layer('ff')[1](
            tparams, proj_h, options, prefix='ff_logit_lstm', activ='linear')
        if options['prev2out']:
            logit += emb
        if options['ctx2out']:
            logit += self.get_layer('ff')[1](
                tparams, ctxgltms, options, prefix='ff_logit_ctxglm', activ='linear')
        logit = tanh(logit)
        if options['use_dropout']:
            logit = dropout_layer(logit, use_noise, trng)
        if options['n_layers_out'] > 1:
            for lidx in xrange(1, options['n_layers_out']):
                logit = self.get_layer('ff')[1](
                    tparams, logit, options, prefix='ff_logit_h%d'%lidx, activ='rectifier')
                if options['use_dropout']:
                    logit = dropout_layer(logit, use_noise, trng)
        # (t,m,n_words)
        logit = self.get_layer('ff')[1](
            tparams, logit, options, prefix='ff_logit', activ='linear')
        logit_shp = logit.shape
        # (t*m, n_words)
        probs = tensor.nnet.softmax(
            logit.reshape([logit_shp[0]*logit_shp[1], logit_shp[2]]))
        # cost
        x_flat = x.flatten() # (t*m,)
        cost = -tensor.log(probs[T.arange(x_flat.shape[0]), x_flat] + 1e-8)

        cost = cost.reshape([x.shape[0], x.shape[1]])
        cost = (cost * mask).sum(0)
        extra = [probs, alphals, alphags,alphams,alphalts]
        return trng, use_noise, x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl,ctxm, mask_ctxm, alphals, alphags, alphams, alphalts,cost, extra

    def build_sampler(self, tparams, options, use_noise, trng, mode=None):
        # context_global: #annotations x dim(f, dim_ctxg)
        ctxg_0 = tensor.matrix('ctxg_sampler', dtype='float32')
        #ctx0.tag.test_value = numpy.random.uniform(size=(50,1024)).astype('float32')
        ctxg_mask = tensor.vector('ctxg_mask', dtype='float32')#f
        #ctx_mask.tag.test_value = numpy.random.binomial(n=1,p=0.5,size=(50,)).astype('float32')
 
        #context_local: (f,n,dim_ctxl)
        ctxl_0 = tensor.tensor3('ctxl_sampler', dtype='float32')
        ctxl_mask = tensor.matrix('ctxl_mask', dtype='float32')
		
        #context_motion:
        ctxm_0 = tensor.matrix('ctxm_sampler', dtype='float32')
        #ctx0.tag.test_value = numpy.random.uniform(size=(50,1024)).astype('float32')
        ctxm_mask = tensor.vector('ctxm_mask', dtype='float32')#f
        #ctx_mask.tag.test_value = numpy.random.binomial(n=1,p=0.5,size=(50,)).astype('float32')

        ctxg_ = ctxg_0
        ctxl_ = ctxl_0
        ctxm_ = ctxm_0
        counts = ctxg_mask.sum(-1)

        if options['encoder'] == 'lstm_bi':
            # encoder
            ctx_fwd = self.get_layer('lstm')[1](tparams, ctx_,
                                           options, mask=ctx_mask,
                                           prefix='encoder',forget=False)[0]
            ctx_rev = self.get_layer('lstm')[1](tparams, ctx_[::-1],
                                           options, mask=ctx_mask[::-1],
                                           forget=False,
                                           prefix='encoder_rev')[0]
            ctx = concatenate((ctx_fwd, ctx_rev[::-1]), axis=1)
            ctx = concatenate((ctx_, ctx),axis=1)
            ctx_mean = ctx.sum(0)/counts
            ctx = ctx.dimshuffle('x',0,1)
        elif options['encoder'] == 'lstm_uni':
            ctx = self.get_layer('lstm')[1](tparams, ctx_,
                                           options,
                                           mask=ctx_mask,
                                           prefix='encoder')[0]
            ctx_mean = ctx.sum(0)/counts
            ctx = ctx.dimshuffle('x',0,1)
        else:
            # do not use RNN encoder
            ctxg = ctxg_
            ctxl = ctxl_
            ctxm = ctxm_
            ctxg_mean = ctxg.sum(0)/counts
            #ctx_mean = ctx.mean(0)
            #ctxg = ctxg.dimshuffle('x',0,1)

        # initial state/cell
        for lidx in xrange(options['n_layers_init']):
            ctx_mean = self.get_layer('ff')[1](
                tparams, ctx1_mean, options, prefix='ff_init_%d'%lidx, activ='rectifier')
            if options['use_dropout']:
                ctx_mean = dropout_layer(ctx_mean, use_noise, trng)
        init_state = [self.get_layer('ff')[1](
            tparams, ctxg_mean, options, prefix='ff_state', activ='tanh')]
        init_memory = [self.get_layer('ff')[1](
            tparams, ctxg_mean, options, prefix='ff_memory', activ='tanh')]
        # ctxg = self.get_layer('ff')[1](
        #     tparams, ctxg, options, prefix='ff_global', activ='tanh')
        ctxl = self.get_layer('ff')[1](
            tparams, ctxl, options, prefix='ff_local', activ='tanh')
        ctxm = self.get_layer('ff')[1](
            tparams, ctxm, options, prefix='ff_motion', activ='tanh')
        ctxg = ctxg.dimshuffle('x', 0, 1)
        ctxl = ctxl.dimshuffle('x', 0, 1, 2)
        ctxm = ctxm.dimshuffle('x', 0, 1)

        print 'Building f_init...',
        f_init = theano.function(
            [ctxg_0, ctxg_mask],
            [ctxg_0]+init_state+init_memory, name='f_init',
            on_unused_input='ignore',
            profile=False, mode=mode)
        print 'Done'

        x = tensor.vector('x_sampler', dtype='int64')
        init_state = [tensor.matrix('init_state', dtype='float32')]
        init_memory = [tensor.matrix('init_memory', dtype='float32')]

        # if it's the first word, emb should be all zero
        emb = tensor.switch(x[:,None] < 0, tensor.alloc(0., 1, tparams['Wemb'].shape[1]),
                            tparams['Wemb'][x])

        proj = self.get_layer('lstm_cond')[1](tparams, emb, options,
                                         prefix='decoder',
                                         mask=None, context_global=ctxg,context_local=ctxl,context_motion=ctxm,
                                         one_step=True,
                                         init_state=init_state[0],
                                         init_memory=init_memory[0],
                                         trng=trng,
                                         use_noise=use_noise,
                                         mode=mode)
        next_state, next_memory, ctxgltms = [proj[0]], [proj[1]], [proj[10]]

        if options['use_dropout']:
            proj_h = dropout_layer(proj[0], use_noise, trng)
        else:
            proj_h = proj[0]
        logit = self.get_layer('ff')[1](
            tparams, proj_h, options, prefix='ff_logit_lstm', activ='linear')
        if options['prev2out']:
            logit += emb
        if options['ctx2out']:
            logit += self.get_layer('ff')[1](
                tparams, ctxgltms[-1], options, prefix='ff_logit_ctxglm', activ='linear')
        logit = tanh(logit)
        if options['use_dropout']:
            logit = dropout_layer(logit, use_noise, trng)
        if options['n_layers_out'] > 1:
            for lidx in xrange(1, options['n_layers_out']):
                logit = self.get_layer('ff')[1](
                    tparams, logit, options, prefix='ff_logit_h%d'%lidx, activ='rectifier')
                if options['use_dropout']:
                    logit = dropout_layer(logit, use_noise, trng)
        logit = self.get_layer('ff')[1](
            tparams, logit, options, prefix='ff_logit', activ='linear')
        logit_shp = logit.shape
        next_probs = tensor.nnet.softmax(logit)
        next_sample = trng.multinomial(pvals=next_probs).argmax(1)

        # next word probability
        print 'building f_next...'
        f_next = theano.function(
            [x, ctxg_0, ctxg_mask, ctxl_0,ctxl_mask, ctxm_0,ctxm_mask]+init_state+init_memory,
            [next_probs, next_sample]+next_state+next_memory,
            name='f_next', profile=False, mode=mode, on_unused_input='ignore')
        print 'Done'
        return f_init, f_next

    def gen_sample(self, tparams, f_init, f_next, ctxg_0, ctxg_mask, ctxl_0, ctxl_mask, ctxm_0,ctxm_mask,options,
                   trng=None, k=1, maxlen=30, stochastic=False,
                   restrict_voc=False):
        '''
		beam search
        ctx0: (26,1024)
        ctx_mask: (26,)
     
        restrict_voc: set the probability of outofvoc words with 0, renormalize
        '''

        if k > 1:
            assert not stochastic, 'Beam search does not support stochastic sampling'

        sample = []
        sample_score = []
        if stochastic:
            sample_score = 0

        live_k = 1
        dead_k = 0

        hyp_samples = [[]] * live_k
        hyp_scores = numpy.zeros(live_k).astype('float32')
        hyp_states = []
        hyp_memories = []

        # [(26,1024),(512,),(512,)]
        rval = f_init(ctxg_0, ctxg_mask)
        ctxg_0 = rval[0]

        next_state = []
        next_memory = []
        n_layers_lstm = 1
        
        for lidx in xrange(n_layers_lstm):
            next_state.append(rval[1+lidx])
            next_state[-1] = next_state[-1].reshape([live_k, next_state[-1].shape[0]])
        for lidx in xrange(n_layers_lstm):
            next_memory.append(rval[1+n_layers_lstm+lidx])
            next_memory[-1] = next_memory[-1].reshape([live_k, next_memory[-1].shape[0]])
        next_w = -1 * numpy.ones((1,)).astype('int64')
        # next_state: [(1,512)]
        # next_memory: [(1,512)]
        for ii in xrange(maxlen):
            # return [(1, 50000), (1,), (1, 512), (1, 512)]
            # next_w: vector
            # ctx: matrix
            # ctx_mask: vector
            # next_state: [matrix]
            # next_memory: [matrix]
            rval = f_next(*([next_w, ctxg_0, ctxg_mask, ctxl_0, ctxl_mask, ctxm_0, ctxm_mask]+next_state+next_memory))
            next_p = rval[0]
            if restrict_voc:
                raise NotImplementedError()
            next_w = rval[1] # already argmax sorted
            next_state = []
            for lidx in xrange(n_layers_lstm):
                next_state.append(rval[2+lidx])
            next_memory = []
            for lidx in xrange(n_layers_lstm):
                next_memory.append(rval[2+n_layers_lstm+lidx])
            if stochastic:
                sample.append(next_w[0]) # take the most likely one
                sample_score += next_p[0,next_w[0]]
                if next_w[0] == 0:
                    break
            else:
                # the first run is (1,50000)
                cand_scores = hyp_scores[:,None] - numpy.log(next_p)
                cand_flat = cand_scores.flatten()
                ranks_flat = cand_flat.argsort()[:(k-dead_k)]

                voc_size = next_p.shape[1]
                trans_indices = ranks_flat / voc_size # index of row
                word_indices = ranks_flat % voc_size # index of col
                costs = cand_flat[ranks_flat]

                new_hyp_samples = []
                new_hyp_scores = numpy.zeros(k-dead_k).astype('float32')
                new_hyp_states = []
                for lidx in xrange(n_layers_lstm):
                    new_hyp_states.append([])
                new_hyp_memories = []
                for lidx in xrange(n_layers_lstm):
                    new_hyp_memories.append([])

                for idx, [ti, wi] in enumerate(zip(trans_indices, word_indices)):
                    new_hyp_samples.append(hyp_samples[ti]+[wi])
                    new_hyp_scores[idx] = copy.copy(costs[idx])
                    for lidx in xrange(n_layers_lstm):
                        new_hyp_states[lidx].append(copy.copy(next_state[lidx][ti]))
                    for lidx in xrange(n_layers_lstm):
                        new_hyp_memories[lidx].append(copy.copy(next_memory[lidx][ti]))

                # check the finished samples
                new_live_k = 0
                hyp_samples = []
                hyp_scores = []
                hyp_states = []
                for lidx in xrange(n_layers_lstm):
                    hyp_states.append([])
                hyp_memories = []
                for lidx in xrange(n_layers_lstm):
                    hyp_memories.append([])

                for idx in xrange(len(new_hyp_samples)):
                    if new_hyp_samples[idx][-1] == 0:
                        sample.append(new_hyp_samples[idx])
                        sample_score.append(new_hyp_scores[idx])
                        dead_k += 1
                    else:
                        new_live_k += 1
                        hyp_samples.append(new_hyp_samples[idx])
                        hyp_scores.append(new_hyp_scores[idx])
                        for lidx in xrange(n_layers_lstm):
                            hyp_states[lidx].append(new_hyp_states[lidx][idx])
                        for lidx in xrange(n_layers_lstm):
                            hyp_memories[lidx].append(new_hyp_memories[lidx][idx])
                hyp_scores = numpy.array(hyp_scores)
                live_k = new_live_k

                if new_live_k < 1:
                    break
                if dead_k >= k:
                    break

                next_w = numpy.array([w[-1] for w in hyp_samples])
                next_state = []
                for lidx in xrange(n_layers_lstm):
                    next_state.append(numpy.array(hyp_states[lidx]))
                next_memory = []
                for lidx in xrange(n_layers_lstm):
                    next_memory.append(numpy.array(hyp_memories[lidx]))

        if not stochastic:
            # dump every remaining one
            if live_k > 0:
                for idx in xrange(live_k):
                    sample.append(hyp_samples[idx])
                    sample_score.append(hyp_scores[idx])

        return sample, sample_score, next_state, next_memory

    def pred_probs(self, whichset, f_log_probs, verbose=True):
        
        probs = []
        n_done = 0
        NLL = []
        L = []
        if whichset == 'train':
            tags = self.engine.train
            iterator = self.engine.kf_train
        elif whichset == 'valid':
            tags = self.engine.valid
            iterator = self.engine.kf_valid
        elif whichset == 'test':
            tags = self.engine.test
            iterator = self.engine.kf_test
        else:
            raise NotImplementedError()
        n_samples = numpy.sum([len(index) for index in iterator])
        for index in iterator:
            tag = [tags[i] for i in index]
            x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask = data_engine.prepare_data(
                self.engine, tag)
            pred_probs = f_log_probs(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask)
            L.append(mask.sum(0).tolist())
            NLL.append((-1 * pred_probs).tolist())
            probs.append(pred_probs.tolist())
            n_done += len(tag)
            if verbose:
                sys.stdout.write('\rComputing LL on %d/%d examples'%(
                             n_done, n_samples))
                sys.stdout.flush()
        print
        probs = common.flatten_list_of_list(probs)
        NLL = common.flatten_list_of_list(NLL)
        L = common.flatten_list_of_list(L)
        perp = 2**(numpy.sum(NLL) / numpy.sum(L) / numpy.log(2))
        return -1 * numpy.mean(probs), perp

    def train(self,
              random_seed=1234,
              dim_word=256, # word vector dimensionality
              ctxglm_dim=-1, # context vector dimensionality, auto set
              ctxg_dim=-1,#context1,vgg+C3D 8192
              ctxl_dim=-1,#context2,rcnn,4096
              ctxm_dim=-1,
              dim=1000, # the number of LSTM units
              n_layers_out=1,
              n_layers_init=1,
              encoder='none',
              encoder_dim=100,
              prev2out=False,
              ctx2out=False,
              patience=10,
              max_epochs=5000,
              dispFreq=100,
              decay_c=0.,
              alpha_c=0.,
              alpha_entropy_r=0.,
              lrate=0.01,
              selector=False,
              n_words=100000,
              maxlen=100, # maximum length of the description
              optimizer='adadelta',
              clip_c=2.,
              batch_size = 64,
              valid_batch_size = 64,
              save_model_dir='/home/tuyunbin/shiyan3/exp1_FC6/',
              validFreq=10,
              saveFreq=10, # save the parameters after every saveFreq updates
              sampleFreq=10, # generate some samples after every sampleFreq updates
              metric='blue',
              dataset='youtube2text',
              video_feature='googlenet',
              use_dropout=False,
              reload_=False,
              from_dir=None,
              K=10,
              OutOf=240,
              verbose=True,
              debug=True
              ):
        self.rng_numpy, self.rng_theano = common.get_two_rngs()

        model_options = locals().copy()
        if 'self' in model_options:
            del model_options['self']
        model_options = validate_options(model_options)
        with open('%smodel_options.pkl'%save_model_dir, 'wb') as f:
            pkl.dump(model_options, f)

        print 'Loading data'
        self.engine = data_engine.Movie2Caption('attention', dataset,
                                           video_feature,
                                           batch_size, valid_batch_size,
                                           maxlen, n_words,
                                           K, OutOf)

        model_options['ctxglm_dim'] = self.engine.ctxglm_dim
        model_options['ctxg_dim'] = self.engine.ctxg_dim
        model_options['ctxl_dim'] = self.engine.ctxl_dim
        model_options['ctxm_dim'] = self.engine.ctxm_dim

        # set test values, for debugging
        idx = self.engine.kf_train[0]
        [self.x_tv, self.mask_tv,
         self.ctxg_tv, self.ctxg_mask_tv, self.ctxl_tv, self.ctxl_mask_tv,self.ctxm_tv,self.ctxm_mask_tv] = data_engine.prepare_data(
            self.engine, [self.engine.train[index] for index in idx])

        print 'init params'
        t0 = time.time()
        params = self.init_params(model_options)

        # reloading
        if reload_:
            model_saved = from_dir+'/model_best_so_far.npz'
            assert os.path.isfile(model_saved)
            print "Reloading model params..."
            params = load_params(model_saved, params)

        tparams = init_tparams(params)

        trng, use_noise, \
              x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl, ctxm, mask_ctxm, alphals,alphags, alphams,alphalts,\
              cost, extra = \
              self.build_model(tparams, model_options)

        print 'buliding sampler'
        f_init, f_next = self.build_sampler(tparams, model_options, use_noise, trng)
        # before any regularizer
        print 'building f_log_probs'
        f_log_probs = theano.function([x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl,ctxm,mask_ctxm], -cost,
                                      profile=False, on_unused_input='ignore')

        cost = cost.mean()
        if decay_c > 0.:
            decay_c = theano.shared(numpy.float32(decay_c), name='decay_c')
            weight_decay = 0.
            for kk, vv in tparams.iteritems():
                weight_decay += (vv ** 2).sum()
            weight_decay *= decay_c
            cost += weight_decay

        if alpha_c > 0.:
            alpha_c = theano.shared(numpy.float32(alpha_c), name='alpha_c')
            alphag_reg = alpha_c * ((1.-alphags.sum(0))**2).sum(0).mean()
            alphal_reg = alpha_c *((1.-alphals.sum(0))**2).sum(0).mean()
            alpham_reg = alpha_c * ((1.-alphams.sum(0))**2).sum(0).mean()
            alphalt_reg = alpha_c *((1.-alphalts.sum(0))**2).sum(0).mean()
            cost += alphag_reg
            cost += alphal_reg
            cost += alpham_reg
            cost += alphalt_reg

        if alpha_entropy_r > 0:
            alpha_entropy_r = theano.shared(numpy.float32(alpha_entropy_r),
                                            name='alpha_entropy_r')
            alpha1_reg_2 = alpha_entropy_r * (-tensor.sum(alpha1s *
                        tensor.log(alpha1s+1e-8),axis=-1)).sum(0).mean()
            alpha2_reg_2 = alpha_entropy_r * (-tensor.sum(alpha2s *
                                                         tensor.log(alpha2s + 1e-8), axis=-1)).sum(0).mean()
            alpha3_reg_2 = alpha_entropy_r * (-tensor.sum(alpha3s *
                                                          tensor.log(alpha3s + 1e-8), axis=-1)).sum(0).mean()
            cost += alphag_reg_2
            cost +=alphal_reg_2
            cost +=alphalt_reg_2
        else:
            alphag_reg_2 = tensor.zeros_like(cost)
            alphal_reg_2 = tensor.zeros_like(cost)
            alpham_reg_2 = tensor.zeros_like(cost)
            alphalt_reg_2 = tensor.zeros_like(cost)
        print 'building f_alphal'
        f_alphal = theano.function([x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl, ctxm, mask_ctxm],
                                  [alphals, alphal_reg_2],
                                  name='f_alpha2',
                                  on_unused_input='ignore')

        print 'building f_alphag'
        f_alphag = theano.function([x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl, ctxm, mask_ctxm],
                                   [alphags, alphag_reg_2],
                                   name='f_alphag',
                                   on_unused_input='ignore')
								   
        print 'building f_alpham'
        f_alpham = theano.function([x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl, ctxm, mask_ctxm],
                                   [alphams, alpham_reg_2],
                                   name='f_alpham',
                                   on_unused_input='ignore')
								   
        print 'building f_alphalt'
        f_alphalt = theano.function([x, mask, ctxg, mask_ctxg, ctxl, mask_ctxl, ctxm, mask_ctxm],
                                   [alphalts, alphalt_reg_2],
                                   name='f_alphalt',
                                   on_unused_input='ignore')



        print 'compute grad'
        grads = tensor.grad(cost, wrt=itemlist(tparams))
        if clip_c > 0.:
            g2 = 0.
            for g in grads:
                g2 += (g**2).sum()
            new_grads = []
            for g in grads:
                new_grads.append(tensor.switch(g2 > (clip_c**2),
                                               g / tensor.sqrt(g2) * clip_c,
                                               g))
            grads = new_grads

        lr = tensor.scalar(name='lr')
        print 'build train fns'
        f_grad_shared, f_update = eval(optimizer)(lr, tparams, grads,
                                                  [x, mask, ctxg, mask_ctxg,ctxl, mask_ctxl,ctxm,mask_ctxm], cost,
                                                  extra + grads)

        print 'compilation took %.4f sec'%(time.time()-t0)
        print 'Optimization'

        history_errs = []
        # reload history
        if reload_:
            print 'loading history error...'
            history_errs = numpy.load(
                from_dir+'model_best_so_far.npz')['history_errs'].tolist()

        bad_counter = 0

        processes = None
        queue = None
        rqueue = None
        shared_params = None

        uidx = 0
        uidx_best_blue = 0
        uidx_best_valid_err = 0
        estop = False
        best_p = unzip(tparams)
        best_blue_valid = 0
        best_valid_err = 999
        alphags_ratio = []
        alphals_ratio = []
        alphams_ratio = []
        alphalts_ratio = []
        for eidx in xrange(max_epochs):
            n_samples = 0
            train_costs = []
            grads_record = []
            print 'Epoch ', eidx
            for idx in self.engine.kf_train:
                tags = [self.engine.train[index] for index in idx]
                n_samples += len(tags)
                uidx += 1
                use_noise.set_value(1.)

                pd_start = time.time()
                x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask = data_engine.prepare_data(
                    self.engine, tags)
                pd_duration = time.time() - pd_start
                if x is None:
                    print 'Minibatch with zero sample under length ', maxlen
                    continue

                ud_start = time.time()
                rvals = f_grad_shared(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask,ctxm,ctxm_mask)
                cost = rvals[0]
                probs = rvals[1]
                alphals = rvals[2]
                alphags = rvals[3]
                alphams = rvals[4]
                alphalts = rvals[5]
                grads = rvals[6:]
                grads, NaN_keys = grad_nan_report(grads, tparams)
                if len(grads_record) >= 5:
                    del grads_record[0]
                grads_record.append(grads)
                if NaN_keys != []:
                    print 'grads contain NaN'
                    import pdb; pdb.set_trace()
                if numpy.isnan(cost) or numpy.isinf(cost):
                    print 'NaN detected in cost'
                    import pdb; pdb.set_trace()
                # update params
                f_update(lrate)
                ud_duration = time.time() - ud_start

                if eidx == 0:
                    train_error = cost
                else:
                    train_error = train_error * 0.95 + cost * 0.05
                train_costs.append(cost)

                if numpy.mod(uidx, dispFreq) == 0:
                    print 'Epoch ', eidx, 'Update ', uidx, 'Train cost mean so far', \
                      train_error, 'fetching data time spent (sec)', pd_duration, \
                      'update time spent (sec)', ud_duration, 'save_dir', save_model_dir
					  
                    alphals,reg = f_alphal(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm,ctxm_mask)
                    print 'alphal ratio %.3f, reg %.3f'%(
                        alphals.min(-1).mean() / (alphals.max(-1)).mean(), reg)
                    alphags,reg = f_alphag(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm,ctxm_mask)
                    print 'alphag ratio %.3f, reg %.3f'%(
                        alphags.min(-1).mean() / (alphags.max(-1)).mean(), reg)
						
                    alphams,reg = f_alpham(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm,ctxm_mask)
                    print 'alpham ratio %.3f, reg %.3f'%(
                        alphams.min(-1).mean() / (alphams.max(-1)).mean(), reg)
						
                    alphalts, reg = f_alphalt(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm,ctxm_mask)
                    print 'alphalt ratio %.3f, reg %.3f' % (
                        alphalts.min(-1).mean() / (alphalts.max(-1)).mean(), reg)

                if numpy.mod(uidx, saveFreq) == 0:
                    pass

                if numpy.mod(uidx, sampleFreq) == 0:
                    use_noise.set_value(0.)
                    def sample_execute(from_which):
                        print '------------- sampling from %s ----------'%from_which
                        if from_which == 'train':
                            x_s = x
                            mask_s = mask
                            ctxg_s = ctxg
                            ctxg_mask_s = ctxg_mask
                            ctxl_s = ctxl
                            ctxl_mask_s = ctxl_mask
                            ctxm_s = ctxm
                            ctxm_mask_s = ctxm_mask

                        elif from_which == 'valid':
                            idx = self.engine.kf_valid[numpy.random.randint(
                                1, len(self.engine.kf_valid) - 1)]
                            tags = [self.engine.valid[index] for index in idx]
                            x_s, mask_s, ctxg_s, ctxg_mask_s, ctxl_s, ctxl_mask_s, ctxm_s, ctxm_mask_s = data_engine.prepare_data(
                                self.engine, tags)

                        stochastic = False
                        for jj in xrange(numpy.minimum(10, x_s.shape[1])):
                            sample, score, _, _ = self.gen_sample(
                                tparams, f_init, f_next, ctxg_s[jj], ctxg_mask_s[jj],ctxl_s[jj], ctxl_mask_s[jj],ctxm_s[jj], ctxm_mask_s[jj],
                                model_options,
                                trng=trng, k=5, maxlen=30, stochastic=stochastic)
                            if not stochastic:
                                best_one = numpy.argmin(score)
                                sample = sample[best_one]
                                print 'cost',score[best_one]
                            else:
                                sample = sample
                            print 'Truth ',jj,': ',
                            for vv in x_s[:,jj]:
                                if vv == 0:
                                    break
                                if vv in self.engine.word_idict:
                                    print self.engine.word_idict[vv],
                                else:
                                    print 'UNK',
                            print
                            for kk, ss in enumerate([sample]):
                                print 'Sample (', kk,') ', jj, ': ',
                                for vv in ss:
                                    if vv == 0:
                                        break
                                    if vv in self.engine.word_idict:
                                        print self.engine.word_idict[vv],
                                    else:
                                        print 'UNK',
                            print
                    sample_execute(from_which='train')
                    sample_execute(from_which='valid')

                if validFreq != -1 and numpy.mod(uidx, validFreq) == 0:
                    t0_valid = time.time()
                    alphals,_ = f_alphal(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask)
                    ratio1 = alphals.min(-1).mean()/(alphals.max(-1)).mean()
                    alphals_ratio.append(ratio1)
                    numpy.savetxt(save_model_dir+'alphal_ratio.txt',alphals_ratio)

                    alphags,_ = f_alphag(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask)
                    ratio2 = alphags.min(-1).mean()/(alphags.max(-1)).mean()
                    alphags_ratio.append(ratio2)
                    numpy.savetxt(save_model_dir+'alphag_ratio.txt',alphags_ratio)
					
                    alphams,_ = f_alpham(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask)
                    ratio3 = alphams.min(-1).mean()/(alphams.max(-1)).mean()
                    alphams_ratio.append(ratio2)
                    numpy.savetxt(save_model_dir+'alpham_ratio.txt',alphams_ratio)

                    alphalts, _ = f_alphalt(x, mask, ctxg, ctxg_mask, ctxl, ctxl_mask, ctxm, ctxm_mask)
                    ratio4 = alphalts.min(-1).mean() / (alphalts.max(-1)).mean()
                    alphalts_ratio.append(ratio4)
                    numpy.savetxt(save_model_dir + 'alphalt_ratio.txt', alphalts_ratio)

                    current_params = unzip(tparams)
                    numpy.savez(
                             save_model_dir+'model_current.npz',
                             history_errs=history_errs, **current_params)

                    use_noise.set_value(0.)
                    train_err = -1
                    train_perp = -1
                    valid_err = -1
                    valid_perp = -1
                    test_err = -1
                    test_perp = -1
                    if not debug:
                        # first compute train cost
                        if 1:
                            print 'computing cost on trainset'
                            train_err, train_perp = self.pred_probs(
                                    'train', f_log_probs,
                                    verbose=model_options['verbose'])
                        else:
                            train_err = 0.
                            train_perp = 0.
                        if 1:
                            print 'validating...'
                            valid_err, valid_perp = self.pred_probs(
                                'valid', f_log_probs,
                                verbose=model_options['verbose'],
                                )
                        else:
                            valid_err = 0.
                            valid_perp = 0.
                        if 1:
                            print 'testing...'
                            test_err, test_perp = self.pred_probs(
                                'test', f_log_probs,
                                verbose=model_options['verbose']
                                )
                        else:
                            test_err = 0.
                            test_perp = 0.

                    mean_ranking = 0
                    blue_t0 = time.time()
                    scores, processes, queue, rqueue, shared_params = \
                        metrics.compute_score(
                        model_type='attention',
                        model_archive=current_params,
                        options=model_options,
                        engine=self.engine,
                        save_dir=save_model_dir,
                        beam=5, n_process=5,
                        whichset='both',
                        on_cpu=False,
                        processes=processes, queue=queue, rqueue=rqueue,
                        shared_params=shared_params, metric=metric,
                        one_time=False,
                        f_init=f_init, f_next=f_next, model=self
                        )

                    valid_B1 = scores['valid']['Bleu_1']
                    valid_B2 = scores['valid']['Bleu_2']
                    valid_B3 = scores['valid']['Bleu_3']
                    valid_B4 = scores['valid']['Bleu_4']
                    valid_Rouge = scores['valid']['ROUGE_L']
                    valid_Cider = scores['valid']['CIDEr']
                    valid_meteor = scores['valid']['METEOR']
                    test_B1 = scores['test']['Bleu_1']
                    test_B2 = scores['test']['Bleu_2']
                    test_B3 = scores['test']['Bleu_3']
                    test_B4 = scores['test']['Bleu_4']
                    test_Rouge = scores['test']['ROUGE_L']
                    test_Cider = scores['test']['CIDEr']
                    test_meteor = scores['test']['METEOR']
                    print 'computing meteor/blue score used %.4f sec, '\
                      'blue score: %.1f, meteor score: %.1f'%(
                    time.time()-blue_t0, valid_B4, valid_meteor)
                    history_errs.append([eidx, uidx, train_err, train_perp,
                                         valid_perp, test_perp,
                                         valid_err, test_err,
                                         valid_B1, valid_B2, valid_B3,
                                         valid_B4, valid_meteor, valid_Rouge, valid_Cider,
                                         test_B1, test_B2, test_B3,
                                         test_B4, test_meteor, test_Rouge, test_Cider])
                    numpy.savetxt(save_model_dir+'train_valid_test.txt',
                                  history_errs, fmt='%.3f')
                    print 'save validation results to %s'%save_model_dir
                    # save best model according to the best blue or meteor
                    if len(history_errs) > 1 and \
                      valid_B4 > numpy.array(history_errs)[:-1,11].max():
                        print 'Saving to %s...'%save_model_dir,
                        numpy.savez(
                            save_model_dir+'model_best_blue_or_meteor.npz',
                            history_errs=history_errs, **best_p)
                    if len(history_errs) > 1 and \
                      valid_err < numpy.array(history_errs)[:-1,6].min():
                        best_p = unzip(tparams)
                        bad_counter = 0
                        best_valid_err = valid_err
                        uidx_best_valid_err = uidx

                        print 'Saving to %s...'%save_model_dir,
                        numpy.savez(
                            save_model_dir+'model_best_so_far.npz',
                            history_errs=history_errs, **best_p)
                        with open('%smodel_options.pkl'%save_model_dir, 'wb') as f:
                            pkl.dump(model_options, f)
                        print 'Done'
                    elif len(history_errs) > 1 and \
                        valid_err >= numpy.array(history_errs)[:-1,6].min():
                        bad_counter += 1
                        print 'history best ',numpy.array(history_errs)[:,6].min()
                        print 'bad_counter ',bad_counter
                        print 'patience ',patience
                        if bad_counter > patience:
                            print 'Early Stop!'
                            estop = True
                            break

                    if self.channel:
                        self.channel.save()

                    print 'Train ', train_err, 'Valid ', valid_err, 'Test ', test_err, \
                      'best valid err so far',best_valid_err
                    print 'valid took %.2f sec'%(time.time() - t0_valid)
                    # end of validatioin
                if debug:
                    break
            if estop:
                break
            if debug:
                break

            # end for loop over minibatches
            print 'This epoch has seen %d samples, train cost %.2f'%(
                n_samples, numpy.mean(train_costs))
        # end for loop over epochs
        print 'Optimization ended.'
        if best_p is not None:
            zipp(best_p, tparams)

        use_noise.set_value(0.)
        valid_err = 0
        test_err = 0
        if not debug:
            #if valid:
            valid_err, valid_perp = self.pred_probs(
                'valid', f_log_probs,
                verbose=model_options['verbose'])
            #if test:
            #test_err, test_perp = self.pred_probs(
            #    'test', f_log_probs,
            #    verbose=model_options['verbose'])


        print 'stopped at epoch %d, minibatch %d, '\
          'curent Train %.2f, current Valid %.2f, current Test %.2f '%(
              eidx,uidx,numpy.mean(train_err),numpy.mean(valid_err),numpy.mean(test_err))
        params = copy.copy(best_p)
        numpy.savez(save_model_dir+'model_best.npz',
                    train_err=train_err,
                    valid_err=valid_err, test_err=test_err, history_errs=history_errs,
                    **params)

        if history_errs != []:
            history = numpy.asarray(history_errs)
            best_valid_idx = history[:,6].argmin()
            numpy.savetxt(save_model_dir+'train_valid_test.txt', history, fmt='%.4f')
            print 'final best exp ', history[best_valid_idx]

        return train_err, valid_err, test_err

def train_from_scratch(state, channel):
    t0 = time.time()
    print 'training an attention model'
    model = Attention(channel)
    model.train(**state.attention)
    print 'training time in total %.4f sec'%(time.time()-t0)

