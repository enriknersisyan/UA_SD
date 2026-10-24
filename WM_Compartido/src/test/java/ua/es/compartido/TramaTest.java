package ua.es.compartido;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TramaTest {
	@Test
	void lrcDeOK(){
		assertEquals((byte) 0x04, Trama.lrc("OK".getBytes()));

	}

	@Test
	void empaquetarDeOK(){
		byte[] esperando = {0x02, 0x4F,0x4B, 0x03, 0x04};
		assertArrayEquals(esperando, Trama.empaquetar("OK"));
	}
}