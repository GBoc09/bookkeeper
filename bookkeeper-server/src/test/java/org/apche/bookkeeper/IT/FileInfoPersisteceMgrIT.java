package org.apche.bookkeeper.IT;

import org.apache.bookkeeper.bookie.FileInfo;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public  class FileInfoPersisteceMgrIT {

    private IndexPersistenceMgr indexPersistenceMgr;
    private FileInfo fileInfo;
    private FileInfo newFileInfo;
    private FileInfo spyFileInfo;
    private File initialFile;
    private File newLedgerFile;
    private LedgerDirsManager ledgerDirsManager;
    private ServerConfiguration serverConfiguration = new ServerConfiguration();
    private FileChannel fileChannel;
    private String masterKey = "masterKey";


    @Before
    public void setUp() throws Exception {
        System.out.println("SET UP method:");
        // Create temporary directory and file
        Path tempDirectory = Files.createTempDirectory("ledgerDir");
        initialFile = Files.createTempFile(tempDirectory, "InitialFile", ".tmp").toFile();

        // Write a valid header to the file
        try (RandomAccessFile raf = new RandomAccessFile(initialFile, "rw")) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(1024);
            headerBuffer.putInt(FileInfo.SIGNATURE); // "BKLE"
            headerBuffer.putInt(FileInfo.CURRENT_HEADER_VERSION);
            byte[] masterKeyBytes = masterKey.getBytes();
            headerBuffer.putInt(masterKeyBytes.length);
            headerBuffer.put(masterKeyBytes);
            headerBuffer.putInt(0); // Initial state bits, not fenced
            headerBuffer.rewind();
            raf.getChannel().write(headerBuffer);
            raf.seek(1024);
            raf.write("testData".getBytes());
            raf.getFD().sync();
        }

        fileChannel = new RandomAccessFile(initialFile, "rw").getChannel();
//        try (RandomAccessFile raf = new RandomAccessFile(initialFile, "r")) {
//            // Sposta il puntatore all'inizio del contenuto dopo l'header
//            raf.seek(1024); // Assumendo che l'header sia di 1024 byte
//
//            // Leggi i dati effettivi (per esempio, "test data")
//            byte[] buffer = new byte[9]; // Lunghezza di "test data"
//            raf.readFully(buffer);
//
//            // Converte i byte in una stringa e stampa
//            String content = new String(buffer);
//            System.out.println("initialFile content: " + content);
//        }

        // Configure LedgerDirsManager mock
        ledgerDirsManager = mock(LedgerDirsManager.class);

        // Create instance of IndexPersistenceMgr
        indexPersistenceMgr = new IndexPersistenceMgr(
                4096,
                512,
                serverConfiguration,
                new SnapshotMap<>(),
                ledgerDirsManager,
                mock(StatsLogger.class)
        );
        System.out.println("indexPersistenceMgr instance: "+indexPersistenceMgr);

        // real FileInfo object
        fileInfo = new FileInfo(initialFile, masterKey.getBytes(), 0);

        // Mock FileInfo with spy to track method interactions
        spyFileInfo = spy(fileInfo);

        // Set the FileChannel directly via reflection to ensure it is initialized
        Field fcField = FileInfo.class.getDeclaredField("fc");
        fcField.setAccessible(true);
        fcField.set(spyFileInfo, fileChannel);
//        fcField.set(fileInfo, fileChannel);
        assertTrue("FileChannel should be open", fileChannel.isOpen());

//        long sizeOfData = fileChannel.size() - 1024; // Calcola la dimensione del contenuto effettivo
//        System.out.println("Data size calculated: " + sizeOfData);

        // Posiziona il puntatore del canale dopo l'header
        fileChannel.position(1024);
        ByteBuffer byteBuffer[] = new ByteBuffer[1];
        byteBuffer[0] = ByteBuffer.wrap("testData".getBytes());
        spyFileInfo.write(byteBuffer, 0);
//        fileInfo.write(byteBuffer, 0);
        fileChannel.write(byteBuffer);


        //ByteBuffer dataBuffer = ByteBuffer.wrap("testData".getBytes());
        //fileChannel.write(dataBuffer);
        // Sincronizza il canale per assicurarsi che i dati siano effettivamente scritti
        fileChannel.force(true);
        ByteBuffer verifyBuffer = ByteBuffer.allocate(8); // "testData".length()
        fileChannel.position(1024); // Reimposta la posizione per la lettura
        fileChannel.read(verifyBuffer);
        verifyBuffer.flip();
        String writtenContent = new String(verifyBuffer.array());
        System.out.println("Data written to file: " + writtenContent);

        Field sizeField = FileInfo.class.getDeclaredField("sizeSinceLastWrite");
        sizeField.setAccessible(true);
        sizeField.set(spyFileInfo, (fileChannel.size()-1024));
//        sizeField.setLong(fileInfo, (fileChannel.size()-1024));

        ByteBuffer buffer = ByteBuffer.allocate(1033);
        spyFileInfo.read(buffer, 0, true);
//        fileInfo.read(buffer, 0, true);
        for (int i = 0; i < 8; i++) {
            System.out.println("string in fileInfo: "+ (char)buffer.array()[i]);
        }

//        System.out.println("sincronized data: "+fileInfo.getSizeSinceLastWrite());
        System.out.println("sincronized data: "+ spyFileInfo.getSizeSinceLastWrite());

//        spyFileInfo.checkOpen(false);

        // Create a new directory for moving the file
        newLedgerFile = Files.createTempFile(tempDirectory,"newLedgerDir", ".tmp").toFile();

        // Mock behavior for picking a writable directory
        when(ledgerDirsManager.pickRandomWritableDirForNewIndexFile(initialFile.getParentFile())).thenReturn(newLedgerFile);
    }

    @After
    public void tearDown() throws Exception {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
        if (initialFile != null && initialFile.exists()) {
            initialFile.delete();
        }
        if (newLedgerFile != null && newLedgerFile.exists()) {
            newLedgerFile.delete();
        }
    }


    @Test
    public void testMoveLedgerIndexFileInteraction() throws Exception {
        System.out.println("--------------------------------------\n" +
                "TEST testMoveLedgerIndexFileInteraction:");
        // Use reflection to access the private moveLedgerIndexFile method
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Ensure initial file exists
        assertTrue("The initial file should exist before the test.", initialFile.exists());
        System.out.println("Initial File Path: " + initialFile.getAbsolutePath());

        assertTrue("FileChannel should be open", fileChannel.isOpen());

        long sizeSinceLastWrite = fileChannel.size() - 1024; // Dati dopo l'header
        System.out.println("Size since last write (calculated): " + sizeSinceLastWrite);

        // Assicurati che `sizeSinceLastWrite` non sia zero
        assertTrue("Size since last write should not be zero", sizeSinceLastWrite > 0);

        System.out.println("get size since last write: "+fileInfo.getSizeSinceLastWrite());
        ByteBuffer buff = ByteBuffer.allocate(1033);
        fileInfo.read(buff, 0, true);
        for (int i = 0; i < 8; i++) {
            System.out.println("string in fileInfo: "+ (char)buff.array()[i]);
        }
        // Invoke the private method using reflection
        moveMethod.invoke(indexPersistenceMgr, 1033L, fileInfo);

        newLedgerFile = fileInfo.getLf();

        newFileInfo = new FileInfo(fileInfo.getLf(), masterKey.getBytes(), 0);
        System.out.println("fileInfo after moveToNewLocation: "+fileInfo.getLf() + " ********** newFileInfo: "+newFileInfo.getLf());
        ByteBuffer buffer = ByteBuffer.allocate(1033);
        int byteRead = newFileInfo.read(buffer, 0, true);
        System.out.println("byte in newFileInfo: " + newFileInfo.size());


        assertEquals(sizeSinceLastWrite, byteRead);

        // Verifica il contenuto
        buffer.flip();
        byte[] data = new byte[byteRead];
        buffer.get(data);
        String content = new String(data);
        assertEquals("testData", content.trim());


        // Opzionalmente, controlla che il vecchio file sia stato eliminato
        System.out.println("Does initial file exist: " + initialFile.exists());
        assertFalse("The original file should not exist anymore", initialFile.exists());
    }

    @Test
    public void mockIntegrationTest() throws NoSuchMethodException, IOException, InvocationTargetException, IllegalAccessException {
        System.out.println("--------------------------------------\n" +
                "TEST mockIntegrationTest:");
        // Use reflection to access the private moveLedgerIndexFile method
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Ensure initial file exists
        assertTrue("The initial file should exist before the test.", initialFile.exists());
        System.out.println("Initial File Path: " + initialFile.getAbsolutePath());

        assertTrue("FileChannel should be open", fileChannel.isOpen());

        long sizeSinceLastWrite = fileChannel.size() - 1024; // Dati dopo l'header
        System.out.println("Size since last write (calculated): " + sizeSinceLastWrite);

        // Assicurati che `sizeSinceLastWrite` non sia zero
        assertTrue("Size since last write should not be zero", sizeSinceLastWrite > 0);

        System.out.println("get size since last write: "+spyFileInfo.getSizeSinceLastWrite());
        ByteBuffer buff = ByteBuffer.allocate(1033);
        spyFileInfo.read(buff, 0, true);
        for (int i = 0; i < 8; i++) {
            System.out.println("string in fileInfo: "+ (char)buff.array()[i]);
        }
        // Invoke the private method using reflection
        moveMethod.invoke(indexPersistenceMgr, 1033L, spyFileInfo);

        newLedgerFile = spyFileInfo.getLf();

        newFileInfo = new FileInfo(spyFileInfo.getLf(), masterKey.getBytes(), 0);
        System.out.println("fileInfo after moveToNewLocation: "+spyFileInfo.getLf() + " ********** newFileInfo: "+newFileInfo.getLf());
        ByteBuffer buffer = ByteBuffer.allocate(1033);
        int byteRead = newFileInfo.read(buffer, 0, true);
        System.out.println("byte in newFileInfo: " + newFileInfo.size());
        assertNotEquals(sizeSinceLastWrite, byteRead);

        // Verifica il contenuto
        buffer.flip();
        byte[] data = new byte[byteRead];
        buffer.get(data);
        String content = new String(data);
        assertEquals("testData", content.trim());


        // Opzionalmente, controlla che il vecchio file sia stato eliminato
        System.out.println("Does initial file exist: " + initialFile.exists());
        assertFalse("The original file should not exist anymore", initialFile.exists());
    }






























    @Test
    public void testFileInfoSizeUpdate() throws Exception {
        // Mock the size returned by FileInfo
        doReturn(2048L).when(spyFileInfo).getSizeSinceLastWrite();

        // Use reflection to invoke moveLedgerIndexFile
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Invoke the method
        moveMethod.invoke(indexPersistenceMgr, 1345L, spyFileInfo);

        // Verify that moveToNewLocation was called with the correct size
        verify(spyFileInfo, times(1)).moveToNewLocation(any(File.class), eq(2048L));
    }
    @Test
    public void testFileChannelAccessToInitialFile() throws Exception {
        // Assicurati che il file esista e il canale sia aperto
        assertTrue("The initial file should exist before the test.", initialFile.exists());
        assertTrue("FileChannel should be open", fileChannel.isOpen());

        // Buffer per leggere il contenuto
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        // Leggi il contenuto del file usando il FileChannel
        fileChannel.position(1024); // Inizia dalla posizione 1024
        int bytesRead = fileChannel.read(buffer);

        // Passa a modalità di lettura
        buffer.flip();

        // Leggi il contenuto in una stringa
        byte[] data = new byte[bytesRead];
        buffer.get(data);

        // Converte i byte in una stringa e verifica
        String content = new String(data);
        System.out.println("expected data in initilFile: "+ content);
        assertTrue("The file should contain the expected data", content.contains("test data"));

        // Stampa il contenuto per verifica visiva
        System.out.println("Content read from initialFile: " + content);
    }
}
