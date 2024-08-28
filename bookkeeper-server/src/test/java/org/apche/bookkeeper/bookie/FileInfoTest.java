package org.apche.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

import org.apache.bookkeeper.bookie.FileInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class FileInfoTest {

    private FileInfo fileInfo;
    private File newFile;
    private long size;


    private boolean expectedException;
    private boolean deleted = false;

    private enum FILE_STATE {
        VALID, // file created and correctly open
        NULL, // null file
        CLOSE, // close file
        DELETE, // file will be deleted
        RENAME, // file will be renamed
        SAME, // same file
        FC_NULL //file with null file channel

    }
    private enum EXPECTED_SIZE{
        ZERO,   // expectedSize = 0
        EXPECTED_SIZE, // expectedSize = expectedString.length
        MIN_EXPECTED_SIZE   // expectedSize = expectedString.length -1
    }
    private FILE_STATE file_state;
    private EXPECTED_SIZE expected_state;
    private long expectedSize;
    private String masterkey = "";
    private final String expectedString = "testFileInfo";

    public FileInfoTest(FileInfoTuple fileInfoTuple){
        this.size = fileInfoTuple.size();
        this.file_state = fileInfoTuple.file_state();
        this.expected_state = fileInfoTuple.expected_state();
        this.expectedException = fileInfoTuple.expectedException();
    }

    /** Assumed that the start data is at 1024 as in the FileInfo class */
    @Parameterized.Parameters
    public static Collection<FileInfoTuple> getFileInfoTuples() {
        List<FileInfoTest.FileInfoTuple> fileInfoTuples = new ArrayList<>();

        // some scenarios
        fileInfoTuples.add(new FileInfoTuple(1024, FILE_STATE.VALID,EXPECTED_SIZE.ZERO,  false));           // 0 ---> newFile is valid but has different size from the expected
        fileInfoTuples.add(new FileInfoTuple(1035, FILE_STATE.VALID, EXPECTED_SIZE.MIN_EXPECTED_SIZE, false)); // 1 ---> newFile is created and valid but its dimension in minor than the expected size of the string
        fileInfoTuples.add(new FileInfoTuple(Long.MAX_VALUE, FILE_STATE.VALID, EXPECTED_SIZE.EXPECTED_SIZE, false)); // 2 ---> newFile is valid and its size is Long.MAX_VALUE, the string can be inserted without problems
        fileInfoTuples.add(new FileInfoTuple(1035, FILE_STATE.CLOSE, EXPECTED_SIZE.MIN_EXPECTED_SIZE, true)); // 3 ---> no newFile
        fileInfoTuples.add(new FileInfoTuple(-1, FILE_STATE.VALID, EXPECTED_SIZE.ZERO, true));

        fileInfoTuples.add(new FileInfoTuple(Long.MAX_VALUE, FILE_STATE.DELETE, EXPECTED_SIZE.ZERO, true)); // 4 ---> trying to verify the condition when the deletion fails BADUA improvement

        fileInfoTuples.add(new FileInfoTuple(0L, FILE_STATE.RENAME,EXPECTED_SIZE.ZERO, false)); // 5 ---> verifying what happens if we try to rename a file,
        fileInfoTuples.add(new FileInfoTuple(Long.MAX_VALUE, FILE_STATE.SAME, EXPECTED_SIZE.EXPECTED_SIZE, false)); // 6 ---> moving the newFile in the same file
        fileInfoTuples.add(new FileInfoTuple(Long.MAX_VALUE, FILE_STATE.FC_NULL, EXPECTED_SIZE.EXPECTED_SIZE, false)); // 7 ---> file channel of a newFile is null


        return fileInfoTuples;
    }
    private static final class FileInfoTuple{
        private final long size;
        private final EXPECTED_SIZE expected_state;
        private final FILE_STATE file_state;
        private final boolean expectedException;

        private FileInfoTuple(long size, FILE_STATE file_state, EXPECTED_SIZE expected_state, boolean expectedException){
            this.size = size;
            this.file_state = file_state;
            this.expected_state = expected_state;
            this.expectedException = expectedException;
        }

        public long size(){return size;}
        public FILE_STATE file_state(){return file_state;}
        public boolean expectedException(){return expectedException;}
        public EXPECTED_SIZE expected_state(){return  expected_state;}
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        // create directory for test
        File fileDir = new File("testDir/fileInfoTest");
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        File file = new File("testDir/fileInfoTest/file.log");
        if (!file.exists()) {
            file.delete();
        }
    }

    @Before
    public void setUp() throws Exception {
        File fl = createTempFile("testFileInfo");
        this.fileInfo = new FileInfo(fl, masterkey.getBytes(), 0);

        ByteBuffer byteBuffer[] = new ByteBuffer[1];
        byteBuffer[0] = ByteBuffer.wrap(this.expectedString.getBytes());
        this.fileInfo.write(byteBuffer, 0);
        try{
            // create newFile
            switch (this.file_state){
                // for this test use mockito is nt so useful
                case VALID:
                    this.newFile = createTempFile("testFileInfoNew");
                    System.out.println("New file name: "+this.newFile);

                    break;
                case CLOSE:
                    this.newFile = createTempFile("testFileInfoCloseFile");
                    this.newFile.delete();
                    break;
                case NULL:
                    this.newFile = null;
                    break;
                case DELETE:
                    File testFile = mock(File.class);
                    when(testFile.delete()).thenReturn(Boolean.valueOf(false));
                    File temp = createTempFile("temp");
                    when(testFile.getPath()).thenReturn(temp.getPath());
                    when(testFile.getParentFile()).thenReturn(temp.getParentFile());
                    when(testFile.exists()).thenReturn(Boolean.valueOf(true));
                    this.fileInfo = new FileInfo(testFile, masterkey.getBytes(), 0);
                    this.newFile = createTempFile("testInfoDeleteFail");
                    break;
                case RENAME:
                    File renameFile = createTempFile("renameFile");
                    this.fileInfo = new FileInfo(renameFile, masterkey.getBytes(), 0);
                    this.newFile = createTempFile("newRenameFile");
                    break;
                case SAME:
                    this.newFile = fl;
                    break;
                case FC_NULL:
                    File f = mock(File.class);
                    when(f.delete()).thenReturn(true);
                    when(f.getPath()).thenReturn("temp/tempoPath");
                    this.fileInfo = new FileInfo(f, masterkey.getBytes(), 0);
                    this.newFile = createTempFile("testFile");
                    break;
            }
            switch (expected_state){
                case EXPECTED_SIZE:
                    this.expectedSize = this.expectedString.length();
                    System.out.println("EXPECTED_SIZE: " + this.expectedSize );
                    break;
                case ZERO:
                    this.expectedSize = 0;
                    System.out.println("EXPECTED_SIZE_ZERO: " + this.expectedSize );
                    break;
                case MIN_EXPECTED_SIZE:
                    this.expectedSize = this.expectedString.length()-1;
                    System.out.println("MIN_EXPECTED_SIZE: " + this.expectedSize );
                    break;

            }
        }catch (Exception e){
                e.printStackTrace();
        }
    }
    /** It creates a temporary file that will be deleted at the end */
    public File createTempFile(String suffix) throws IOException {
        File file = IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
        return file;
    }

    /** first branch:
     * il test sposta il file in una nuova posizione specificata dalla variabile newFile utilizzando il metodo moveToNewLocation.
     * Successivamente legge i dati dal nuovo file e li confronta con i dati attesi, verificando  anche se il numero
     * di byte letti corrisponde alla dimensione attesa. Inoltre verifica se ci si aspetta che l'operazione generi un'eccezione.
     *
     * RENAME:
     * il test sposta il file in una nuova posizione specificata dalla variabile newFile. In questo caso si assicura che
     * non venga generata alcuna eccezione.
    */
    @Test
    public void testFileInfo() throws IOException {
          if(file_state == FILE_STATE.VALID || file_state == FILE_STATE.NULL||(file_state == FILE_STATE.CLOSE && expected_state == EXPECTED_SIZE.MIN_EXPECTED_SIZE)){
            this.fileInfo.moveToNewLocation(this.newFile, this.size);
            ByteBuffer buffer = ByteBuffer.allocate((int)this.expectedSize+10); // allocate a bigger arrays
            FileInfo fileInfoNew = new FileInfo(this.fileInfo.getLf(), masterkey.getBytes(), 0);
            int byteRead = fileInfoNew.read(buffer, 0, true);
            System.out.println("byte in fileInfoNew: " + fileInfoNew.size());
            System.out.println("expectedSize: " + this.expectedSize+ " " + "byteRead: " + byteRead);
            Assert.assertEquals(byteRead, this.expectedSize);
            for (int i = 0; i< byteRead; i++){
                System.out.println("string in fileInfoNew: "+ (char)buffer.array()[i]);
                Assert.assertEquals(expectedString.charAt(i) + " " + buffer.array()[i], expectedString.charAt(i), (char) buffer.array()[i]);
            }
            if(this.expectedException){
                Assert.assertTrue("New file size -1, can't move to new location", this.expectedException);
            }else {
                Assert.assertFalse(this.expectedException);
            }
        }

        if(file_state == FILE_STATE.RENAME) {
            this.fileInfo.moveToNewLocation(this.newFile, this.size);
            Assert.assertFalse("Expected exception TRUE", this.expectedException);
        }
        if(file_state == FILE_STATE.FC_NULL || file_state == FILE_STATE.SAME){
            this.fileInfo.moveToNewLocation(this.newFile, this.size);
            Assert.assertEquals(this.deleted, fileInfo.isDeleted());
        }
        try {
            if (file_state == FILE_STATE.CLOSE || file_state == FILE_STATE.DELETE) {
                fileInfo.moveToNewLocation(this.newFile, this.size);
                long newSize = fileInfo.size();
                if(newSize>expectedSize){
                    Assert.assertTrue(newSize>this.expectedSize);
                }else if (deleted){
                    Assert.fail();
                }
            }
        }catch (IOException e){
            if(expectedException){
                Assert.assertTrue(expectedException);
            }else {
                Assert.fail();
            }
        }
        System.out.println("End testFileInfo");
    }


    @After
    public void tearDownFile() throws IOException {
        //Delete the test file
        File file = new File("testDir/fileInfoTest/file.log");
        if (file.exists()) {
            file.delete();
        }
    }

    @AfterClass
    public static void tearDown() {

        //Delete directory and test file
        File fileDir = new File("testDir/fileInfoTest");
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

}