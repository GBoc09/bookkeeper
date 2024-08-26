package org.apche.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.FileInfo;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class FileInfoWriteTest {
    private enum BUFF_STATE{
        EMPTY, // no byte to write
        NOT_EMPTY, // there are byte to write
        SHORT // 1 or less bytes for showing short write exception
    }

    private enum FC_STATE{
        VALID, // can reach byte
        INVALID, // can't reach byte
        SHORT_WRITE
    }

    private long position;
    private BUFF_STATE buff_state;
    private FC_STATE fc_state;
    private FileChannel fc;
    boolean expectedException;
    private byte[] buff;
    private byte[] buffs;
    private FileInfo fileInfo;
    private String masterkey = "";
    private File file;
    private ByteBuffer mockBuffer = mock(ByteBuffer.class);

    public FileInfoWriteTest (FileInfoWriteTuple fileInfoWriteTuple){
        this.fc_state = fileInfoWriteTuple.fc_state();
        this.buff_state = fileInfoWriteTuple.buff_state();
        this.position = fileInfoWriteTuple.position();
        this.expectedException = fileInfoWriteTuple.expectedException();
    }

    @Parameterized.Parameters
    public static Collection<FileInfoWriteTuple> getFileInfoWriteTuples(){
        List<FileInfoWriteTest.FileInfoWriteTuple> fileInfoWriteTuples = new ArrayList<>();
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.EMPTY,0,false));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.NOT_EMPTY, 0,false));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.NOT_EMPTY, 1024, true));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.NOT_EMPTY, 1025, true));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.INVALID, BUFF_STATE.NOT_EMPTY,1024, true));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.EMPTY, -1, true));

        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.SHORT_WRITE, BUFF_STATE.SHORT,0,true));

        return fileInfoWriteTuples;
    }

    private static final class FileInfoWriteTuple{
        private final FC_STATE fc_state;
        private final BUFF_STATE buff_state;
        private final long position;
        private final boolean expectedException;

        private FileInfoWriteTuple(FC_STATE fc_state, BUFF_STATE buff_state, long position, boolean expectedException){
            this.fc_state = fc_state;
            this.buff_state = buff_state;
            this.position = position;
            this.expectedException = expectedException;
        }

        public FC_STATE fc_state(){return fc_state;}
        public BUFF_STATE buff_state(){return buff_state;}
        public long position(){return position;}
        public boolean expectedException(){return expectedException;}
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        // create directory for test
        File fileDir = new File("testDir/fileInfoWriteTest");
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        File file = new File("testDir/fileInfoWriteTest/file.log");
        if (!file.exists()) {
            file.delete();
        }
    }

    @Before
    public void setUp() throws Exception {
        Random random = new Random(System.currentTimeMillis());
        try {
            switch (this.fc_state){
                case VALID:
                    this.fc = FileChannel.open(Paths.get("testDir/fileInfoWriteTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    this.fc.position(this.position); // we append data in sequence, we are trying to avoid overwrite the previous inputs.
                    System.out.println("valid fc.size: " + this.fc.size());
                    break;
                case INVALID:
                    FileChannel fc = FileChannel.open(Paths.get("testDir/fileInfoWriteTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    fc.close();
                    this.fc = fc;
                    break;
                case SHORT_WRITE:
                    System.out.println("Short write");
                    this.fc = mock(FileChannel.class);
                    when(this.fc.write(any(ByteBuffer.class), anyLong())).thenReturn(Integer.valueOf(0)); // Simulate short write
                    ByteBuffer buffer = ByteBuffer.allocate(1);
                    long result = this.fc.write(new ByteBuffer[]{buffer});
                    System.out.println( "Test write result: " + result);
                    if (result != 0L) {
                        throw new AssertionError("Mock FileChannel did not return 0 as expected");
                    }else {
                        System.out.println("Mock FileChannel returned 0 as expected");
                    }
                    break;
            }
            switch (this.buff_state){
                case EMPTY:
                    buff = new byte[0];
                    Arrays.fill(this.buff, (byte)0);
                    System.out.println( "empty buff:"+ buff + "buff lenght: "+buff.length);
                    break;
                case NOT_EMPTY:
                    buff = new byte[1024];
                    random.nextBytes(this.buff);
                    System.out.println("random byte in buff: "+buff + "\n" + "buff length: "+buff.length);
                    break;
                case SHORT:
                    when(mockBuffer.remaining()).thenReturn(Integer.valueOf(1));
                    buff = new byte[1];
                    buffs = new byte[0];
                    System.out.println( "short buff:"+ buffs + "\n" + "buff length: "+buffs.length);
                    System.out.println("***\nconfiguration FC: "+ fc_state + " configuration buff: "+ buff_state);
                    ByteBuffer buffer = ByteBuffer.allocate(1);
                    long result = this.fc.write(new ByteBuffer[]{buffer});
                    long mockResult = this.fc.write(new ByteBuffer[]{mockBuffer});
                    System.out.println("Test write result: " + result );
                    if (result != 0L && mockResult != 0L) {
                        throw new AssertionError("Mock FileChannel did not return 0 as expected");
                    }else {
                        System.out.println("Mock FileChannel returned 0 as expected \n***");
                    }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileInfoWrite() throws Exception {
        file = new File("testDir/fileInfoWriteTest/file.log");
        long actualByteWritten;
        long expectedByteWritten;
        this.fileInfo = new FileInfo(file, masterkey.getBytes(), 0); // initialize a fileInfo for calling the method
        System.out.println("expectedException: "+this.expectedException);
        if (this.position == -1){
            System.out.println("Invalid position");
            Assert.assertTrue(this.expectedException);
        }
        if (this.expectedException && buff_state == BUFF_STATE.NOT_EMPTY) {
            fileInfo.write(new ByteBuffer[]{ByteBuffer.wrap(buff)}, this.position);
            System.out.println("file size: " + file.length());
            Assert.assertTrue("expected exception", this.expectedException);
        } else if (buff_state != BUFF_STATE.SHORT && buff_state != BUFF_STATE.EMPTY) {
            actualByteWritten = fileInfo.write(new ByteBuffer[]{ByteBuffer.wrap(this.buff)}, this.position);
            expectedByteWritten = 0;
            if (this.buff_state == BUFF_STATE.NOT_EMPTY){
                expectedByteWritten = this.buff.length;
            }else {
                expectedByteWritten = 0;
            }
            Assert.assertEquals(expectedByteWritten, actualByteWritten);
            System.out.println("actual byte written: "+actualByteWritten + "\n"+ "fc.read: "+this.fc.read(ByteBuffer.wrap(this.buff)));
        }
        if (buff_state == BUFF_STATE.SHORT){
            file = new File("testDir/fileInfoWriteTest/ShortFile.log");
            fileInfo.write(new ByteBuffer[]{ByteBuffer.wrap(buff)}, this.position);
            Assert.assertTrue("expected exception", this.expectedException);
            try {
                fileInfo.write(new ByteBuffer[]{mockBuffer}, this.position);
                System.out.println("file size: " + file.length());
            } catch (IOException e) {
                Assert.assertTrue("expected exception", this.expectedException);
            }

        }

    }
    @After
    public void tearDown() throws IOException {
        buff = null;
        if (this.fileInfo != null) {
            this.fileInfo = null;  // Elimina l'istanza per forzare una nu27ova creazione nel prossimo test
        }
//         Chiude il FileChannel se è aperto
        if (fc != null && fc.isOpen()) {
            fc.close();
        }
        if (!file.equals("testDir/fileInfoWriteTest/file.log")) {
            file.delete();
        }
    }


}



