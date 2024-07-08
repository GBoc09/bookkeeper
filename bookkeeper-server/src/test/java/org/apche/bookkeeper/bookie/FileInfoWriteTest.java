package org.apche.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.FileInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(value = Parameterized.class)
public class FileInfoWriteTest {
    private enum BUFF_STATE{
        EMPTY, // no byte to write
        NOT_EMPTY // there are byte to write
    }

    private enum FC_STATE{
        VALID, // can reach byte
        INVALID, // can't reach byte
        ERROR, // it returns -1 because there is an error in the writings
        SHORT_WRITE
    }

    private long position;
    private BUFF_STATE buff_state;
    private FC_STATE fc_state;
    private FileChannel fc;
    boolean expectedException;
    private byte[] buff;
    private FileInfo fileInfo;
    private String masterkey = "";

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
        //fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.VALID, BUFF_STATE.EMPTY, -1, true));

        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.SHORT_WRITE, BUFF_STATE.NOT_EMPTY,0,true));
        fileInfoWriteTuples.add(new FileInfoWriteTuple(FC_STATE.ERROR, BUFF_STATE.NOT_EMPTY,0,true));


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
                    this.fc = mock(FileChannel.class);
                    when(this.fc.write(any(ByteBuffer.class), anyLong())).thenReturn(0); // Simulate short write
                    break;
                case ERROR:
                    this.fc = mock(FileChannel.class);
                    when(this.fc.write(any(ByteBuffer.class), anyLong())).thenReturn(-1); // Simulate error
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
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileInfoWrite() throws Exception {
        File file = new File("testDir/fileInfoWriteTest/file.log");
        long actualByteWritten;
        long expectedByteWritten;
        this.fileInfo = new FileInfo(file, masterkey.getBytes(), 0); // initialize a fileInfo for calling the method
        System.out.println("expectedException: "+this.expectedException);
        if (this.expectedException) {
            fileInfo.write(new ByteBuffer[]{ByteBuffer.wrap(buff)}, this.position);
            Assert.assertTrue("expected exception", this.expectedException);
        } else {
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
    }
}



