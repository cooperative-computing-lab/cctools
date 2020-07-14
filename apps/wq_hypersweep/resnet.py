import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import numpy as np
import datetime as dt
import os.path
from os import path
import sys
import getopt


# Default hyperparamter values
BATCHSIZE = 64
RESNETBLOCKS = 10
DROPOUTRATE = 0.5
EPOCHS = 30
EPOCHSTEPS = 195
VALIDATIONSTEPS = 3 

# Command line changes to hyperparams
try:
	opts, args = getopt.getopt(sys.argv[1:],"hb:r:d:e:s:v",[])
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


#LOG_DIR = './log'
#get_ipython().system_raw(
#    'tensorboard --logdir {} --host 0.0.0.0 --port 6006 &'
#    .format(LOG_DIR)
#)


# retrieve dataset
(x_train, y_train), (x_test, y_test) = tf.keras.datasets.cifar10.load_data()


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


#callbacks = [
#   Write TensorBoard logs to `./logs` directory
#  keras.callbacks.TensorBoard(log_dir='./log/{}'.format(dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")), write_images=True),
#]



# Define ResNet model
inputs = keras.Input(shape=(24, 24, 3))
x = layers.Conv2D(32, 3, activation='relu')(inputs)
x = layers.Conv2D(64, 3, activation='relu')(x)
x = layers.MaxPooling2D(3)(x)

num_res_net_blocks = RESNETBLOCKS
for i in range(num_res_net_blocks):
  x = res_net_block(x, 64, 3)

x = layers.Conv2D(64, 3, activation='relu')(x)
x = layers.GlobalAveragePooling2D()(x)
x = layers.Dense(256, activation='relu')(x)
x = layers.Dropout(DROPOUTRATE)(x)
outputs = layers.Dense(10, activation='softmax')(x)

res_net_model = keras.Model(inputs, outputs)


# Build ResNet model
res_net_model.compile(optimizer=keras.optimizers.Adam(),
              loss='sparse_categorical_crossentropy',
              metrics=['acc'])
#res_net_model.fit(train_dataset, epochs=EPOCHS, steps_per_epoch=EPOCHSTEPS,
#          validation_data=valid_dataset,
#          validation_steps=3, callbacks=callbacks)

# Train ResNet model
res_net_model.fit(train_dataset, epochs=EPOCHS, steps_per_epoch=EPOCHSTEPS,
          validation_data=valid_dataset,
          validation_steps=VALIDATIONSTEPS)

# Perform evaluation for result reporting
evaluation = res_net_model.evaluate(valid_dataset, steps=VALIDATIONSTEPS)

# Report results
output = str(evaluation[0])+", "+str(evaluation[1])+", "+str(BATCHSIZE)+", "+str(RESNETBLOCKS)+", "+str(DROPOUTRATE)+", "+str(EPOCHS)+", "+str(EPOCHSTEPS)+", "+str(VALIDATIONSTEPS)+"\n"

if (not path.exists("results.csv")):
	results = open("results.csv", "w")
	results.write("loss, accuracy, batch_size, num_res_net_blocks, dropout_rate, epochs, steps_per_epoch, validation_steps \n")
	results.close()

results = open("results.csv", "a")
results.write(output)
results.close()
