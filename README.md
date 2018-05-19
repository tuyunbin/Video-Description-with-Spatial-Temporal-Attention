# Video-Description-with-Spatial-Temporal-Attention
This package contains the accompanying code for the following paper:

Tu, Yunbin, et al. ["Video Description with Spatial-Temporal Attention."](http://delivery.acm.org/10.1145/3130000/3123354/p1014-tu.pdf?ip=58.60.1.113&id=3123354&acc=ACTIVE%20SERVICE&key=BF85BBA5741FDC6E%2E0871A888CCEFF346%2EE1B7C59A421B1D76%2E4D4702B0C3E38B35&__acm__=1524795092_f95c862de249b8599ebe872d9bfe4c2d) which has appeared as full paper in the Proceedings of the ACM International Conference on Multimedia,2017 (ACM MM'17).

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

[Theano](http://deeplearning.net/software/theano/install.html) can be easily installed by following the instructions there. Theano has its own dependencies as well. The second way to install Theano is to install Anaconda. If you use first way to install Theao, you may meet the error : "no module named pygpu". If so, you should install it with Anaconda, but you needn't change your python environment. You only add this command when you use Theano:
```
$ export PATH="/home/tuyunbin/anaconda2/bin:$PATH"
```
(Changing your own PATH)

[coco-caption](https://github.com/tylin/coco-caption). Install it by simply adding it into your $PYTHONPATH.

[Jobman](http://deeplearning.net/software/jobman/install.html). After it has been git cloned, please add it into $PYTHONPATH as well.

Finally, you will also need to install [h5py](https://pypi.org/project/h5py/), since we will use hdf5 files to store the preprocessed features.

### Video Datas and Pre-extracted Features on MSVD Dataset.

[The pre-processed datasets used in our paper are available at this links](http://www.multcloud.com/share/050e69cd-cab9-4ba3-a671-ed459341ab41)

The pre-processed global, motion and local features used in our paper can be download at these links:

[global and motion features](http://www.multcloud.com/share/c86f1a5e-e3e5-459d-8af2-e615a3818a0b)

[local features](http://www.multcloud.com/share/08a3d104-1c61-4047-8045-931879106ffd)

In our paper, we used local features extracted from the fc7 layer of Faster R-CNN network, and their number is 8. You can extract local features with other number by [Faster R-CNN](https://github.com/rbgirshick/py-faster-rcnn).

Note: Since the data amount on MSR-VTT-10K is too large, we don't offer the data we used. You can train your model on this dataset with the same code. But don't forget to shuffle the train_id when training the model. 

### Test model trained by us

[Firstly, you need to download the pre-trained model at this link](http://www.multcloud.com/share/e31c7520-c44c-450e-93c6-f367a235575b), and add them into $PYTHONPATH. 

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

### Notes

Running train_model.py for the first time takes much longer since Theano needs to compile for the first time lots of things and cache on disk for the future runs. You will probably see some warning messages on stdout. It is safe to ignore all of them. Both model parameters and configurations are saved (the saving path is printed out on stdout, easy to find). The most important thing to monitor is train_valid_test.txt in the exp output folder. It is a big table saving all metrics per validation. 

### Contact
My email is tuyunbin1995@foxmail.com

Any discussions and suggestions are welcome!
