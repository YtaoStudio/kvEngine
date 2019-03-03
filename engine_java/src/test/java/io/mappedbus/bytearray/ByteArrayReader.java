package io.mappedbus.bytearray;
import io.mappedbus.MappedBusReader;

import java.util.Arrays;

import static io.mappedbus.MappedBudConstant.FILE_NAME;

public class ByteArrayReader {

	public static void main(String[] args) {
		ByteArrayReader reader = new ByteArrayReader();
		reader.run();
	}

	public void run() {
		try {
			MappedBusReader reader = new MappedBusReader(FILE_NAME, 2000000L, 10);
			reader.open();

			byte[] buffer = new byte[10];

			int i = 0;
			while (true) {
				if (reader.next()) {
                    int length = reader.readBuffer(buffer, 0);
                    System.out.println("Read: length = " + length + ", data= "+ Arrays.toString(buffer));
                }else{
                    break;
                }
                i++;
            }
            System.out.println("DONE: ");
        } catch(Exception e) {
			e.printStackTrace();
		}
	}
}