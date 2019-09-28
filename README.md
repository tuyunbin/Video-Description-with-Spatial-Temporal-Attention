# Video-Description-with-Spatial-Temporal-Attention
This package contains the accompanying code for the following paper:

Tu, Yunbin, et al. ["Video Description with Spatial-Temporal Attention."](https://dl.acm.org/citation.cfm?id=3123266.3123354), and ["Baidu Cloud"](https://www.multcloud.com/share/e795d4a3-75ab-4906-829e-eaa90a91b4b1), which has appeared as full paper in the Proceedings of the ACM International Conference on Multimedia,2017 (ACM MM'17). 

The codes are forked from [yaoli/arctic-capgen-vid](https://github.com/yaoli/arctic-capgen-vid).

## We illustrate the training details as follows:

## usage

### Installation

Firstly, Clone our repository:
```
$ git clone https://github.com/tuyunbin/Video-Description-with-Spatial-Temporal-Attention.git
```

Here, msvd_data contains 7 pkl files needed to train and test the model.
### Dependencies

[Theano](http://deeplearning.net/software/theano/install.html) can be easily installed by following the instructions there. Theano has its own dependencies as well. The second way to install Theano is to install Anaconda. If you use first way to install Theano, you may meet the error : "no module named pygpu". If so, you should install it with Anaconda, but you needn't change your python environment. You only add this command when you use Theano:
```
$ export PATH="/home/tuyunbin/anaconda2/bin:$PATH"
```
(Changing your own PATH)

[coco-caption](https://github.com/tylin/coco-caption). Install it by simply adding it into your $PYTHONPATH.

[Jobman](http://deeplearning.net/software/jobman/install.html). After it has been git cloned, please add it into $PYTHONPATH as well.

Finally, you will also need to install [h5py](https://pypi.org/project/h5py/), since we will use hdf5 files to store the preprocessed features.

### Video Datas and Pre-extracted Features on MSVD Dataset.

[The pre-processed datasets used in our paper are available at this links](https://drive.google.com/file/d/1LyfN6s8xKju-iad8M3OvaqFeoPT4aQV9/view?usp=sharing)

The pre-processed global, motion and local features used in our paper can be download at these links:

[global features](https://drive.google.com/file/d/1tiZg3q7RJtMJbFzgUeS0NyGsfuXce8yh/view?usp=sharing)

[motion features](https://drive.google.com/file/d/1U0Spn3dsDamhDT_Akx-ySHvpZlgOsx9B/view?usp=sharing)

[local features](https://pan.baidu.com/s/1TvZL0ktP2tMxNJV4kYCLeg)

In our paper, we used local features extracted from the fc7 layer of Faster R-CNN network, and their number is 8. You can extract local features with other number by [Faster R-CNN](https://github.com/rbgirshick/py-faster-rcnn).

Note: Since the data amount on MSR-VTT-10K is too large, we don't offer the data we used. You can train your model on this dataset with the same code. But don't forget to shuffle the train_id when training the model. 

### Test model trained by us

[Firstly, you need to download the pre-trained model at this link](https://pan.baidu.com/s/1lGCDpqd0pbb5ot1P-Nw4bg), and add them into your $PYTHONPATH. 

Secondly, go to common.py and change the following two line 
```
RAB_DATASET_BASE_PATH = '/home/tuyunbin/Video-Description-with-Spatial-Temporal-Attention/msvd_data/' 
RAB_EXP_PATH = '/home/sdc/tuyunbin/msvd_result/Video-Description-with-Spatial-Temporal-Attention/exp/' 
```
according to your specific setup. The first path is the parent dir path containing msvd_data folder. The second path specifies where you would like to save all the experimental results.
Before testing the model, we suggest to test ```data_engine.py``` by running python data_engine.py without any error.
It is also useful to verify coco-caption evaluation pipeline works properly by running ```python metrics.py``` without any error.

Finally, you can exploit our trained model by setting this configuration with 'True' in ```config.py```.
```
'reload_': True,
```

### Train your own model
Here, you need to set 'False' with reload in ```config.py```.

Now ready to launch the training
```
$ THEANO_FLAGS=mode=FAST_RUN,device=cuda0,floatX=float32 python train_model.py
```

If you find this helps your research, please consider citing:
```
@inproceedings{tu2017video,
  title={Video Description with Spatial-Temporal Attention},
  author={Tu, Yunbin and Zhang, Xishan and Liu, Bingtao and Yan, Chenggang},
  booktitle={Proceedings of the 2017 ACM on Multimedia Conference},
  pages={1014--1022},
  year={2017},
  organization={ACM}
}
```

### Notes

Running train_model.py for the first time takes much longer since Theano needs to compile for the first time lots of things and cache on disk for the future runs. You will probably see some warning messages on stdout. It is safe to ignore all of them. Both model parameters and configurations are saved (the saving path is printed out on stdout, easy to find). The most important thing to monitor is train_valid_test.txt in the exp output folder. It is a big table saving all metrics per validation. 

### Contact
My email is tuyunbin1995@foxmail.com

Any discussions and suggestions are welcome!
