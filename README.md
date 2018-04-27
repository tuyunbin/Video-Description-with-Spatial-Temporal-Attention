# Video-Description-with-Spatial-Temporal-Attention
This package contains the accompanying code for the following paper:

Tu, Yunbin, et al. ["Video Description with Spatial-Temporal Attention."](http://delivery.acm.org/10.1145/3130000/3123354/p1014-tu.pdf?ip=58.60.1.113&id=3123354&acc=ACTIVE%20SERVICE&key=BF85BBA5741FDC6E%2E0871A888CCEFF346%2EE1B7C59A421B1D76%2E4D4702B0C3E38B35&__acm__=1524795092_f95c862de249b8599ebe872d9bfe4c2d) Proceedings of the 2017 ACM on Multimedia Conference. ACM, 2017.

The codes are forked from [yaoli/arctic-capgen-vid](https://github.com/yaoli/arctic-capgen-vid).

#####We illustrate the training details, which can also be found in their repo. 

## usage

### Dependencies

[Theano](http://deeplearning.net/software/theano/install.html) can be easily installed by following the instructions there. Theano has its own dependencies as well. The simpliest way to install Theano is to install Anaconda. Instead of using Theano coming with [Anaconda](https://www.anaconda.com/download/), we suggest running git clone git://github.com/Theano/Theano.git to get the most recent version of Theano.

[coco-caption](https://github.com/tylin/coco-caption). Install it by simply adding it into your $PYTHONPATH.
[Jobman](http://deeplearning.net/software/jobman/install.html). After it has been git cloned, please add it into $PYTHONPATH as well.
