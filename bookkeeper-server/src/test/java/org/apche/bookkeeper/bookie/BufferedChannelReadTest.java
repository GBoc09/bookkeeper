package org.apche.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {
    public static final Class<? extends Exception> SUCCESS = null;
    private FileChannel fileChannel;

    private final int capacity;

    private ByteBuf dest;

    private final int fileSize;

    private final int length;
    private final int position;

    private enum FC_STATE{
        EMPTY,
        NOT_EMPTY,
        INVALID,
        NULL
    }
    private enum DEST_STATE{
        VALID,
        INVALID,
        NULL
    }

    private FC_STATE fc_state;
    private DEST_STATE dest_state;

    private byte [] dataToBeRead;

    private final boolean writeBeforeRead;

    private Random random = new Random(System.currentTimeMillis());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    public BufferedChannelReadTest(ReadInputTuple readInputTuple) {
        this.capacity = readInputTuple.capacity();
        this.position = readInputTuple.position();
        this.length = readInputTuple.length();
        this.fileSize = readInputTuple.fileSize();
        this.fc_state = readInputTuple.fc_state();
        this.dest_state = readInputTuple.dest_state();
        this.writeBeforeRead = readInputTuple.writeBeforeRead();
        if(readInputTuple.expectedException() != null){
            this.expectedException.expect(readInputTuple.expectedException());
        }
    }

    @Parameterized.Parameters
    public static Collection<ReadInputTuple> getReadInputTuples() {
        List<ReadInputTuple> readInputTupleList = new ArrayList<>();
        //                                              {capacity    fc_state    dest_state           position       length  fileSize   writeBeforeRead          exception}
        readInputTupleList.add(new ReadInputTuple(-1, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 0, 11, 12, false, Exception.class)); // how the method handles a negative value
        readInputTupleList.add(new ReadInputTuple(0, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 0, 11, 12, false, Exception.class)); // capacity 0
        //readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 0, 11, 12, false, SUCCESS)); //read more bytes than available --> READ MORE BYTES disabled for building the project
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NULL, DEST_STATE.VALID, 0, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.INVALID, DEST_STATE.VALID, 0, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID, 0, 0, 0, false, SUCCESS));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID, 0, 1, 0, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.INVALID, 0, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.NULL, 0, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 12, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 13, 11, 12, false, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 0, 12, 12, false, SUCCESS));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.NOT_EMPTY, DEST_STATE.VALID, 0, 13, 12, false, Exception.class));

        //readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID, 0,1, 2, true, SUCCESS)); // READ MORE BYTES THAN NEEDED

        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID, 0, 1, 0, true, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID,13, 11, 12, true, Exception.class));
        readInputTupleList.add(new ReadInputTuple(10, FC_STATE.EMPTY, DEST_STATE.VALID, 0, 0, 0, true, SUCCESS));



        return readInputTupleList;
    }
    private static final class ReadInputTuple {
        private final int capacity; // buffered channel capacity
        private final FC_STATE fc_state;
        private final DEST_STATE dest_state;
        private final int position; // where the next write operation will begin
        private final int length; // how long is the remaining part
        private final int fileSize;
        private final boolean writeBeforeRead;
        private final Class<? extends Exception> expectedException;

        private ReadInputTuple(int capacity,
                               FC_STATE fc_state,
                               DEST_STATE dest_state,
                               int position,
                               int length,
                               int fileSize,
                               boolean writeBeforeRead,
                               Class<? extends Exception> expectedException) {
            this.capacity = capacity;
            this.fc_state = fc_state;
            this.dest_state = dest_state;
            this.position = position;
            this.length = length;
            this.fileSize = fileSize;
            this.writeBeforeRead = writeBeforeRead;
            this.expectedException = expectedException;
        }
        public int capacity() {return capacity;}
        public FC_STATE fc_state(){
            return fc_state;}
        public DEST_STATE dest_state(){return dest_state;}
        public int position(){return position;}
        public int length(){return length;}
        public int fileSize(){return fileSize;}
        public boolean writeBeforeRead(){return writeBeforeRead;}
        public Class<? extends Exception> expectedException(){return expectedException;}

    }


    @BeforeClass
    public static void setUpClass() throws Exception {
        // create directory for test
        File fileDir = new File("testDir/bufferedChannelReadTest");
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        File file = new File("testDir/bufferedChannelReadTest/file.log");
        if (!file.exists()) {
            file.delete();
        }
    }
    @Before
    public void setUp() {
        //Create FileChannel
        try {
            if (this.fc_state == FC_STATE.NOT_EMPTY || this.fc_state == FC_STATE.EMPTY) {
                this.dataToBeRead = new byte[this.fileSize];
                random.nextBytes(this.dataToBeRead);
                if (this.fc_state == FC_STATE.NOT_EMPTY) {
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/bufferedChannelReadTest/file.log")) {
                        fileOutputStream.write(this.dataToBeRead);
                    }
                }

                this.fileChannel = FileChannel.open(Paths.get("testDir/bufferedChannelReadTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                this.fileChannel.position(this.fileChannel.size());

                if (this.fc_state == FC_STATE.NULL) {
                    this.fileChannel = null;
                } else if (this.fc_state == FC_STATE.INVALID) {
                    this.fileChannel.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Create ByteBuf
        switch(this.dest_state) {
            case VALID:
                this.dest = Unpooled.buffer();
                break;
            case NULL:
                this.dest = null;
                break;
            case INVALID:
                this.dest = mock(ByteBuf.class);
                when(this.dest.writableBytes()).thenReturn(1);
                when(this.dest.writeBytes(any(ByteBuf.class), any(int.class), any(int.class))).thenThrow(new IndexOutOfBoundsException("Invalid ByteBuf"));
        }

    }

    @After
    public void tearDownFile() {
        try {
            //Close the FileChannel
            if (this.fc_state != BufferedChannelReadTest.FC_STATE.NULL && this.fileChannel != null) {
                this.fileChannel.close();
            }
            //Delete the test file
            File file = new File("testDir/bufferedChannelReadTest/file.log");
            if (file.exists()) {
                file.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {

        //Delete directory and test file
        File fileDir = new File("testDir/bufferedChannelReadTest");
        if (fileDir.exists()) {
            File[] files = fileDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
        fileDir.delete();

        File directory = new File("testDir");
        if (directory.exists()) {
            directory.delete();
        }
    }


    /** con il flag writeBeforeRead == TRUE viene scritto un certo numero di byte nel bufferedChannel prima di eseguire
     * il test di lettura. Questo simula un'operazione di scrittura precedente nel buffer, che potrebbe andare a modificare le condizione
     * in cui vengono effettuati i test di lettura.
     * Viene invocata la read() del BufferedChannel, fornendo la posiozione di lettura desiderata e la lunghezza della lettura.
     * Il metodo read() legge i dati dal fileChannel nella posizione specificata nel buffer di destinazione.
     * I dati letti dal fileChannel vengono confrontati con i dati attesi che sono stati determinati in base hai parametri
     * dei test, eseguiamo questo confronto per garantire che il metodo read() abbia letto correttamente i dati e nel modo previsto.
     * */
    @Test
    public void readTest() throws IOException {
        BufferedChannel bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.capacity);
        if(this.writeBeforeRead){
            ByteBuf tempByteBuf = Unpooled.buffer();
            tempByteBuf.writeBytes(this.dataToBeRead);
            bufferedChannel.write(tempByteBuf);
        }
        Integer actualNumOfBytesRead = bufferedChannel.read(this.dest, this.position, this.length);
        Integer expectedNumOfBytesInReadBuff = 0;
        byte[] expectedBytes = new byte[0];
        if (this.position <= this.fileChannel.size()) {
            if(this.length > 0) {
                if(writeBeforeRead){
                    expectedNumOfBytesInReadBuff = Math.toIntExact((this.length+1 - this.position >= this.length) ? this.length : this.length+1 - this.position - this.length);
                    expectedBytes = Arrays.copyOfRange(this.dataToBeRead, this.position, this.position + expectedNumOfBytesInReadBuff);
                }else {
                    expectedNumOfBytesInReadBuff = Math.toIntExact((this.fileChannel.size() - this.position >= this.length) ? this.length : this.fileChannel.size() - this.position - this.length);
                    expectedBytes = Arrays.copyOfRange(this.dataToBeRead, this.position, this.position + expectedNumOfBytesInReadBuff);
                }
            }
        }
        byte[] actualBytesRead = new byte[0];
        if(this.dest_state == DEST_STATE.VALID) {
            actualBytesRead = Arrays.copyOfRange(this.dest.array(), 0, actualNumOfBytesRead);
        }

        Assert.assertEquals("BytesRead Check Failed", Arrays.toString(expectedBytes), Arrays.toString(actualBytesRead));
        Assert.assertEquals("NumOfBytesRead Check Failed", expectedNumOfBytesInReadBuff, actualNumOfBytesRead);
    }
}
