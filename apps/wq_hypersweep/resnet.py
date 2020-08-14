from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.python.keras import backend as K
from tensorflow.python.keras.datasets.cifar import load_batch
from tensorflow.python.keras.utils.data_utils import get_file
from tensorflow.python.util.tf_export import keras_export

import numpy as np
import datetime as dt
import os
import os.path
from os import path
import sys
import getopt


# Limit Tensorflow to single thread execution
tf.config.threading.set_intra_op_parallelism_threads(1)
tf.config.threading.set_inter_op_parallelism_threads(1)


def load_cifar_data():

	dirname = 'cifar-10-batches-py'
	path = "datasets/"+dirname

	if os.path.exists(path):
		print("Local data found.")

	else:
		print("Local data not found. Retrieving from source.")
		origin = 'https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz'
		get_file(
			dirname,
			origin=origin,
			untar=True,
			file_hash='6d958be074577803d12ecdefd02955f39262c83c16fe9348329d7fe0b5c001ce',
			cache_dir=os.getcwd()),

	num_train_samples = 50000

	x_train = np.empty((num_train_samples, 3, 32, 32), dtype='uint8')
	y_train = np.empty((num_train_samples,), dtype='uint8')

	for i in range(1, 6):
		fpath = os.path.join(path, 'data_batch_' + str(i))
		(x_train[(i - 1) * 10000:i * 10000, :, :, :],y_train[(i - 1) * 10000:i * 10000]) = load_batch(fpath)

	fpath = os.path.join(path, 'test_batch')
	x_test, y_test = load_batch(fpath)

	y_train = np.reshape(y_train, (len(y_train), 1))
	y_test = np.reshape(y_test, (len(y_test), 1))

	if K.image_data_format() == 'channels_last':
		x_train = x_train.transpose(0, 2, 3, 1)
		x_test = x_test.transpose(0, 2, 3, 1)

	x_test = x_test.astype(x_train.dtype)
	y_test = y_test.astype(y_train.dtype)

	return (x_train, y_train), (x_test, y_test)



def res_net_block(input_data, filters, conv_size):
	x = layers.Conv2D(filters, conv_size, activation='relu', padding='same')(input_data)
	x = layers.BatchNormalization()(x)
	x = layers.Conv2D(filters, conv_size, activation=None, padding='same')(x)
	x = layers.BatchNormalization()(x)
	x = layers.Add()([x, input_data])
	x = layers.Activation('relu')(x)
	return x



def non_res_block(input_data, filters, conv_size):
	x = layers.Conv2D(filters, conv_size, activation='relu', padding='same')(input_data)
	x = layers.BatchNormalization()(x)
	x = layers.Conv2D(filters, conv_size, activation='relu', padding='same')(x)
	x = layers.BatchNormalization()(x)
	return x



def main():

	# Default hyperparamter values
	BATCHSIZE = 64
	RESNETBLOCKS = 10
	DROPOUTRATE = 0.5
	EPOCHS = 30
	EPOCHSTEPS = 195
	VALIDATIONSTEPS = 3
	OUTPUT_FILE = 'results.csv'

	# Command line changes to hyperparams
	try:
		opts, args = getopt.getopt(sys.argv[1:],"hb:r:d:e:s:v:o:",[])
	except getopt.GetoptError:
		print('usage: test.py -b <batch size> -r <number of ResNet blocks> -d <dropout rate> -e <number of epochs> -s <steps per epoch> -v <validation steps>')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print('usage: test.py -b <batch size> -r <number of ResNet blocks> -d <dropout rate> -e <number of epochs> -s <steps per epoch> -v <validation steps>')
			sys.exit()
		elif opt in ("-b"):
			BATCHSIZE = int(arg)
		elif opt in ("-r"):
			RESNETBLOCKS = int(arg)
		elif opt in ("-d"):
			DROPOUTRATE = float(arg)
		elif opt in ("-e"):
			EPOCHS = int(arg)
		elif opt in ("-s"):
			EPOCHSTEPS = int(arg)
		elif opt in ("-v"):
			VALIDATIONSTEPS = int(arg)
		elif opt in ("-o"):
			OUTPUT_FILE = str(arg)

	# Retrieve dataset
	(x_train, y_train), (x_test, y_test) = load_cifar_data()

	# Partition training set into batches, perform center cropping to improve performance and training time
	train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train)).batch(BATCHSIZE).shuffle(10000)
	train_dataset = train_dataset.map(lambda x, y: (tf.cast(x, tf.float32) / 255.0, y))
	train_dataset = train_dataset.map(lambda x, y: (tf.image.central_crop(x, 0.75), y))
	train_dataset = train_dataset.map(lambda x, y: (tf.image.random_flip_left_right(x), y))
	train_dataset = train_dataset.repeat()

	# Partition validation set into batches, perform center cropping
	valid_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test)).batch(5000).shuffle(10000)
	valid_dataset = valid_dataset.map(lambda x, y: (tf.cast(x, tf.float32) / 255.0, y))
	valid_dataset = valid_dataset.map(lambda x, y: (tf.image.central_crop(x, 0.75), y))
	valid_dataset = valid_dataset.repeat()

	# Define ResNet model
	inputs = keras.Input(shape=(24, 24, 3))
	x = layers.Conv2D(32, 3, activation='relu')(inputs)
	x = layers.Conv2D(64, 3, activation='relu')(x)
	x = layers.MaxPooling2D(3)(x)

	for i in range(RESNETBLOCKS):
		x = res_net_block(x, 64, 3)

	x = layers.Conv2D(64, 3, activation='relu')(x)
	x = layers.GlobalAveragePooling2D()(x)
	x = layers.Dense(256, activation='relu')(x)
	x = layers.Dropout(DROPOUTRATE)(x)
	outputs = layers.Dense(10, activation='softmax')(x)

	res_net_model = keras.Model(inputs, outputs)


	# Build ResNet model
	res_net_model.compile(optimizer=keras.optimizers.Adam(),loss='sparse_categorical_crossentropy',metrics=['acc'])

	# Train ResNet model
	res_net_model.fit(train_dataset, epochs=EPOCHS, steps_per_epoch=EPOCHSTEPS,validation_data=valid_dataset,validation_steps=VALIDATIONSTEPS)

	# Perform evaluation for result reporting
	evaluation = res_net_model.evaluate(valid_dataset, steps=VALIDATIONSTEPS)

	# Report results
	output = str(evaluation[0])+", "+str(evaluation[1])+", "+str(BATCHSIZE)+", "+str(RESNETBLOCKS)+", "+str(DROPOUTRATE)+", "+str(EPOCHS)+", "+str(EPOCHSTEPS)+", "+str(VALIDATIONSTEPS)+"\n"

	if (not path.exists(OUTPUT_FILE)):
		results = open(OUTPUT_FILE, "w")
		results.write("loss, accuracy, batch_size, num_res_net_blocks, dropout_rate, epochs, steps_per_epoch, validation_steps \n")
		results.close()

	results = open(OUTPUT_FILE, "a")
	results.write(output)
	results.close()


if __name__ == "__main__":
	main()
