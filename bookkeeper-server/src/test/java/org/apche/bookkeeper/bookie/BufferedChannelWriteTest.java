package org.apche.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** we want to verify assure that the write method complete its execution */

@RunWith(Parameterized.class) /** this class will use Parameterized so we can run this test with multiple input sets.   */
public class BufferedChannelWriteTest {
    /** TO SEE THE REPORT (building only the server package) OPEN THE TARGET FOLDER IN THE BOOKKEEPER SERVER MODULE. */

    public static final Class<? extends Exception> SUCCESS = null;
    private FileChannel fileChannel;

    private int capacity; // writeBuffer capacity

    private ByteBuf src;
    private int srcSize;

    private byte[] data;

    private enum STATE {
        EMPTY,
        NOT_EMPTY,
        NULL,
        INVALID
    }

    private STATE  fc_state;
    private STATE src_state;

    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

    private long unpersistedBytes;
    private int numberOfExistingBytes;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelWriteTest(WriteInputTuple writeInputTuple){
        this.capacity = writeInputTuple.capacity();
        this.srcSize = writeInputTuple.srcSize();
        this.fc_state = writeInputTuple.fc_state();
        this.src_state = writeInputTuple.src_state();
        this.unpersistedBytes = writeInputTuple.unpersistedBytes();
        this.numberOfExistingBytes = 0;
        if (writeInputTuple.expectedException() != null) {
            this.expectedException.expect(writeInputTuple.expectedException());
        }
    }

    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples() {
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        //(CAPACITY,   SRCSIZE,   FC_STATE,   SRC_STATE,          UNPERSISTEDBYTES,   EXCEPTION)
        writeInputTupleList.add(new WriteInputTuple(-1, 5, STATE.EMPTY, STATE.NOT_EMPTY, 0L, Exception.class));
//        writeInputTupleList.add(new WriteInputTuple(0, 5, STATE.EMPTY, STATE.NOT_EMPTY, 0L, Exception.class)); // infinity loop --> there is a problem in the original method
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE.EMPTY, STATE.NOT_EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 10, STATE.EMPTY, STATE.NOT_EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE.EMPTY, STATE.NOT_EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE.EMPTY, STATE.EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(0, 0, STATE.EMPTY, STATE.EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE.EMPTY, STATE.NULL, 0L, Exception.class));
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE.EMPTY, STATE.INVALID, 0L, Exception.class));
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE.NOT_EMPTY, STATE.NOT_EMPTY, 0L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE.NULL, STATE.NOT_EMPTY, 0L, Exception.class));
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE.INVALID, STATE.NOT_EMPTY, 0L, Exception.class));

        //JACOCO REPORT
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE.EMPTY, STATE.NOT_EMPTY, 3L, SUCCESS));
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE.EMPTY, STATE.NOT_EMPTY, 6L, SUCCESS));
        // PIT REPORT
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE.EMPTY, STATE.NOT_EMPTY, 5L, SUCCESS));

        return writeInputTupleList;
    }

    private static final class WriteInputTuple {
        private final int capacity; // file channel capacity
        private final int srcSize; // buffer src size
        private final STATE fc_state; // file channel state
        private final STATE src_state; // buffer src state
        private final Class<? extends Exception> expectedException;
        private final long unpersistedBytes; // remaining unflush byte

        private WriteInputTuple (int capacity, int srcSize, STATE fc_state, STATE src_state, long unpersistedBytes, Class<? extends Exception> expectedException) {
            this.capacity = capacity;
            this.srcSize = srcSize;
            this.fc_state = fc_state;
            this.src_state = src_state;
            this.unpersistedBytes = unpersistedBytes;
            this.expectedException = expectedException;
        }
        public int capacity() {return capacity;}
        public int srcSize() {return srcSize;}
        public STATE fc_state() {return fc_state;}
        public STATE src_state() {return src_state;}
        public Class<? extends Exception> expectedException() {return expectedException;}
        public long unpersistedBytes() {return unpersistedBytes;}
    }

    @BeforeClass
    public static void setUpClass()  {
        // create directory for test
        File fileDir = new File("testDir/bufferedChannelWriteTest");
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        File file = new File("testDir/bufferedChannelWriteTest/file.log");
        if (!file.exists()) {
            file.delete();
        }
    }

    @Before
    public void setUp()  {
        //Create FileChannel
        try{
            Random random = new Random(System.currentTimeMillis());
            if(this.fc_state == STATE.NOT_EMPTY || this.fc_state == STATE.EMPTY){
                if(this.fc_state == STATE.NOT_EMPTY){
                    try(FileOutputStream fileOutputStream = new FileOutputStream("testDir/bufferedChannelWriteTest/file.log")){
                        this.numberOfExistingBytes = random.nextInt(10);
                        byte[] alreadyExistingBytes = new byte[this.numberOfExistingBytes]; // random byte array. We fill it with random num of bytes between 0 and 9
                        random.nextBytes(alreadyExistingBytes);
                        fileOutputStream.write(alreadyExistingBytes); // write the random bytes on the Output stream file.log
                    }
                }
                this.fileChannel = FileChannel.open(Paths.get("testDir/bufferedChannelWriteTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                this.fileChannel.position(this.fileChannel.size()); // we append data in sequence, we are trying to avoid overwrite the previous inputs.
                this.data = new byte[this.srcSize]; // initializes data with srcSize slots.

                if(this.src_state != STATE.EMPTY){
                    random.nextBytes(this.data); // fills data with random bytes
                } else {
                    Arrays.fill(this.data, (byte)0); // fills data with value 0 because it's EMPTY
                }
            } else if (this.src_state == STATE.NULL) {
                this.fileChannel = null;
            } else if (this.src_state == STATE.INVALID){
                FileChannel fc = FileChannel.open(Paths.get("testDir/bufferedChannelWriteTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.close();
                this.fileChannel = fc;
            }
            this.src = Unpooled.directBuffer(this.srcSize);
            if(this.src_state == STATE.NOT_EMPTY) {
                this.src.writeBytes(this.data);
            } else if (this.src_state == STATE.NULL){
                this.src = null;
            } else if (this.src_state == STATE.INVALID) {
                ByteBuf invalidByteBuf = mock(ByteBuf.class);
                when(invalidByteBuf.readableBytes()).thenReturn(1);
                when(invalidByteBuf.readerIndex()).thenReturn(-1);
                this.src = invalidByteBuf;
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    @After
    public void tearDownFile() {

        try {
            //Close the FileChannel
            if (this.fc_state != STATE.NULL && this.fileChannel != null) {
                this.fileChannel.close();
            }
            //Delete the test file
            File file = new File("testDir/bufferedChannelWriteTest/file.log");
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
        File fileDir = new File("testDir/bufferedChannelWriteTest");
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

    /** istanzia un oggetto Buffered Channel, usiamo il metodo write() per scrivere i dati contenuti nel buffer src
     * sul Buffered Channel.
     * Calcoliamo le dimensioni attese cioè il numero di byte che ci si aspetta di trovare all'interno del buffer di
     * scrittura e nel file channel dopo la scrittura.
     * Estraiamo i byte effettivamente presenti nel buffer di scrittura e nel file channel e li confronta con i dati attesi.
     * Facciamo questo per verificare che tutti i dati siano stati scritti correttamente.
     *
     * Usiamo gli assert per per verificare se i byte presenti nel buffer di scrittura e nel file channel corrispondono a
     * quelli attesi. Inoltre verifca se la posizione corrente del BufferdChannel è stata aggiornata correttamente  dopo
     * la scrittura dei dati. */
    @Test
    public void writeTest() throws IOException {
        System.out.println("Capacity: " + this.capacity);
        BufferedChannel bufferedChannel = new BufferedChannel(this.allocator, this.fileChannel, this.capacity, this.unpersistedBytes);
        bufferedChannel.write(this.src);
        System.out.println("Byte in BufferedChannel: " + bufferedChannel.size());

        int expectedNumByteInWriteBuff = 0;
        if (this.src_state != STATE.EMPTY && capacity != 0) {
            if (this.srcSize < this.capacity) {
                expectedNumByteInWriteBuff = this.srcSize;
            } else {
                expectedNumByteInWriteBuff = this.srcSize % this.capacity; // at least capacity bytes can be written in the buffer
                System.out.println("expectedNumByteInWriteBuff: " + expectedNumByteInWriteBuff);
            }
        }

        int expectedNumByteInFileChannel = 0;
        if (this.unpersistedBytes > 0L) { // only if we cross the threshold we can write directly in the FC
            if (this.unpersistedBytes <= this.srcSize) {
                expectedNumByteInFileChannel = this.srcSize;
                expectedNumByteInWriteBuff = 0;
            }
        } else { // we are under the threshold
            if (this.srcSize < this.capacity) {
                expectedNumByteInFileChannel = 0; // we write only in the write buffer
            } else {
                expectedNumByteInFileChannel = this.srcSize - expectedNumByteInWriteBuff; // only the unpersisted byte are written in the FC
                System.out.println("expectedNumByteInFileChannel: " + expectedNumByteInFileChannel);
            }
        }

        byte[] actualBytesInWriteBuff = new byte[expectedNumByteInWriteBuff];
        bufferedChannel.writeBuffer.getBytes(0, actualBytesInWriteBuff);

        byte[] effectiveByteInWriteBuff = Arrays.copyOfRange(this.data, this.data.length - expectedNumByteInWriteBuff, this.data.length);


        System.out.println("Actual Bytes in Buffered Channel : " + Arrays.toString(actualBytesInWriteBuff));
        System.out.println("Expected Bytes in Buffered Channel : " + Arrays.toString(effectiveByteInWriteBuff));

        Assert.assertEquals("BytesInWriteBuff Check Failed", Arrays.toString(actualBytesInWriteBuff), Arrays.toString(effectiveByteInWriteBuff));

        ByteBuffer actualByteInFC = ByteBuffer.allocate(expectedNumByteInFileChannel);
        this.fileChannel.position(this.numberOfExistingBytes);
        this.fileChannel.read(actualByteInFC);

        System.out.println(data.toString());
        byte[] expectedByteInFC = Arrays.copyOfRange(this.data, 0, expectedNumByteInFileChannel);

        System.out.println("Actual Bytes in File Channel: " + Arrays.toString(actualByteInFC.array()));
        System.out.println("Expected Bytes in File Channel: " + Arrays.toString(expectedByteInFC));

        Assert.assertEquals("BytesInFC Check Failed", Arrays.toString(actualByteInFC.array()), Arrays.toString(expectedByteInFC));

        if (this.src_state == STATE.EMPTY) {
            Assert.assertEquals("BufferedChannelPosition Check Failed", this.numberOfExistingBytes, bufferedChannel.position());
        } else {
            Assert.assertEquals("BufferedChannelPosition Check Failed", this.numberOfExistingBytes + this.srcSize, bufferedChannel.position());
        }
    }



}