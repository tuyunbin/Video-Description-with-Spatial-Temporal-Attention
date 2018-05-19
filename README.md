# Video-Description-with-Spatial-Temporal-Attention
This package contains the accompanying code for the following paper:

Tu, Yunbin, et al. ["Video Description with Spatial-Temporal Attention."](http://delivery.acm.org/10.1145/3130000/3123354/p1014-tu.pdf?ip=58.60.1.113&id=3123354&acc=ACTIVE%20SERVICE&key=BF85BBA5741FDC6E%2E0871A888CCEFF346%2EE1B7C59A421B1D76%2E4D4702B0C3E38B35&__acm__=1524795092_f95c862de249b8599ebe872d9bfe4c2d) which has appeared as full paper in the Proceedings of the ACM International Conference on Multimedia,2017 (ACM MM'17).

The codes are forked from [yaoli/arctic-capgen-vid](https://github.com/yaoli/arctic-capgen-vid).

## We illustrate the training details as follows:

## usage

### Dependencies
Firstly, Clone the STAT repository:



[Theano](http://deeplearning.net/software/theano/install.html) can be easily installed by following the instructions there. Theano has its own dependencies as well. The second way to install Theano is to install Anaconda. If you use first way to install Theao, you may meet the erro : "no module named pygpu". If so, you should install it with Anaconda, but you needn't change your python environment. Adding this command will be fine when you use Theano:
```
export PATH="/home/tuyunbin/anaconda2/bin:$PATH"
```
(Changing your own PATH)

[coco-caption](https://github.com/tylin/coco-caption). Install it by simply adding it into your $PYTHONPATH.

[Jobman](http://deeplearning.net/software/jobman/install.html). After it has been git cloned, please add it into $PYTHONPATH as well.

Finally, you will also need to install [h5py](https://pypi.org/project/h5py/), since we will use hdf5 files to store the preprocessed data.

### Video Datas and Pre-extracted Features on MSVD Dataset.

#### MSVD

[The pre-processed datasets used in our paper are available at this links](http://www.multcloud.com/share/050e69cd-cab9-4ba3-a671-ed459341ab41

[The pre-processed global, motion and local features used in our paper can be download at this link](http://www.multcloud.com/share/21a1a8c8-f2df-4a68-8e6b-9be1c6f8d669)
In our paper, we used local features are extracted from the fc7 layer of Faster R-CNN network, and their number is 8. Here, we additionally provide local features with number 10, since the effect with number 10 is better accoring to comparison experiments. Besides, we attempt to use another local fatures called label features. We extracted top-n objects name by Faster R-CNN and transformed these name into label fatures with pre-trained Glove model (i.e. these label features are word representation of top-n object name), and their dimension are 300. Here, we find that it achieve better results when setting their number is 6.



