import RPi.GPIO as GPIO
import time

class Lamp:
    pins={'r':23, 'g':24, 'b':25}

    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(pins['r'], GPIO.OUT)
    GPIO.setup(pins['g'], GPIO.OUT)
    GPIO.setup(pins['b'], GPIO.OUT)
    #print(f'init {pins}')

    def on(self, pin):
        for p in pin: GPIO.output(self.pins[p], GPIO.HIGH)

    def off(self, pin):
        for p in pin: GPIO.output(self.pins[p], GPIO.LOW)
