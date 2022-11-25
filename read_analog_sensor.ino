const int figaro=27; //Trimethyl amine  TGS2603
const int ch3sh=25;  //Methyl mercaptan

void setup() {
	Serial.begin(115200);
	Serial2.begin(9600);
	pinMode(2, OUTPUT);
	Serial.printf("\n\nBegin ADC ESP32, Kyuho Kim, 2022/11/18");
}

void read_and_fire() {
	int figaro_v = analogRead(figaro);
	int ch3sh_v = analogRead(ch3sh);
	String data="{\"2603\":"+ String(figaro_v) +",\"ch3sh\":"+ String(ch3sh_v) +"}";
	Serial.println(data);
	Serial2.println(data);
}

void loop() {
	while (Serial2.available()) {
		digitalWrite(2, HIGH);
		String s=Serial2.readString();
		s.trim();
		if (s=="[ENQ]") read_and_fire();
		digitalWrite(2, LOW);
	}
}
