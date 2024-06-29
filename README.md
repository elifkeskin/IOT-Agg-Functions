# IOT Agg Functions
 Spark Streaming Agg Functions

Dataset Story:

The data was generated from a series of three identical, custom-built, breadboard-based sensor arrays. Each array was connected to a Raspberry Pi devices. Each of the three IoT devices was placed in a physical location with varied environmental conditions.

Goal:

Write a sparkstreaming application (in local or yarn mode) that prints the Sensor (device) id, how many signals were generated, Average CO and humidity values ​​on the screen, scrolling every 5 minutes in the last 10-minute window of each sensor (device), based on the event time.

Data Fields:

ts: timestamp of reading.

device: unique device name.

co:carbon monoxide.

humidity: humidity (%)

light: light detected?

lpg: liquefied petroleum gas

motion: motion detected?

smoke: smoke

temp:temperature(F)
