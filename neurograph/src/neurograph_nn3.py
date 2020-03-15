import os
import json

import numpy as np
from keras.models import Sequential
from keras.layers.core import Dense, Flatten
from keras.layers.normalization import BatchNormalization
from keras.layers.convolutional import MaxPooling2D, Conv2D

from nn_node import NNNode
from base_service import ServiceUtils, RmqInputInfo, RmqOutputInfo


class NN3(NNNode):
    def _handle_message(self, message: str):
        data = json.loads(message)
        digit = np.array(data['digit']).reshape((-1, 28, 28, 1))

        print("Received: ", digit[0][0], "...")
        print("Previous net output: ", data["out_layer"])

        model = self.create_model()
        predicts = model.predict(digit)[0]
        out_layer = [round(x, 5) for x in predicts]

        self._send(json.dumps({"digit": data["digit"], "out_layer": out_layer}))

    def create_model(self):
        model = Sequential()

        model.add(Conv2D(filters=64, kernel_size=(3, 3), activation="relu", input_shape=(28, 28, 1)))
        model.add(Conv2D(filters=64, kernel_size=(3, 3), activation="relu"))

        model.add(MaxPooling2D(pool_size=(2, 2)))
        model.add(BatchNormalization())
        model.add(Conv2D(filters=128, kernel_size=(3, 3), activation="relu"))
        model.add(Conv2D(filters=128, kernel_size=(3, 3), activation="relu"))

        model.add(MaxPooling2D(pool_size=(2, 2)))
        model.add(BatchNormalization())
        model.add(Conv2D(filters=256, kernel_size=(3, 3), activation="relu"))

        model.add(MaxPooling2D(pool_size=(2, 2)))

        model.add(Flatten())
        model.add(BatchNormalization())
        model.add(Dense(512, activation="relu"))

        model.add(Dense(10, activation="softmax"))

        model.compile(loss="categorical_crossentropy", optimizer="adam", metrics=["accuracy"])
        model.load_weights(f"{os.environ.get('MODELS_PATH')}/mnist.h5")

        return model


nn3 = NN3(input=RmqInputInfo('NN3'), outputs=[RmqOutputInfo('NN4')])
ServiceUtils.start_service(nn3, __name__)
