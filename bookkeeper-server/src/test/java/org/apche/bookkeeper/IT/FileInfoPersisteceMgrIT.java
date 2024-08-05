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
            raf.write("test data".getBytes());
            raf.getFD().sync();
        }

        fileChannel = new RandomAccessFile(initialFile, "rw").getChannel();
        try (RandomAccessFile raf = new RandomAccessFile(initialFile, "r")) {
            // Sposta il puntatore all'inizio del contenuto dopo l'header
            raf.seek(1024); // Assumendo che l'header sia di 1024 byte

            // Leggi i dati effettivi (per esempio, "test data")
            byte[] buffer = new byte[9]; // Lunghezza di "test data"
            raf.readFully(buffer);

            // Converte i byte in una stringa e stampa
            String content = new String(buffer);
            System.out.println("initialFile content: " + content);
        }
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
        fcField.set(fileInfo, fileChannel);
        assertTrue("FileChannel should be open", fileChannel.isOpen());

        long sizeOfData = fileChannel.size() - 1024; // Calcola la dimensione del contenuto effettivo
        System.out.println("Data size calculated: " + sizeOfData);

//        spyFileInfo.checkOpen(false);

        // Create a new directory for moving the file
        newLedgerFile = Files.createTempFile(tempDirectory,"newLedgerDir", ".tmp").toFile();

        newFileInfo = new FileInfo(fileInfo.getLf(), masterKey.getBytes(), 0);

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

    @Test
    public void testMoveLedgerIndexFileInteraction() throws Exception {
        // Use reflection to access the private moveLedgerIndexFile method
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Ensure initial file exists
        assertTrue("The initial file should exist before the test.", initialFile.exists());
        System.out.println("Initial File Path: " + initialFile.getAbsolutePath());

        assertTrue("FileChannel should be open", fileChannel.isOpen());
//        System.out.println("size since last write: "+fileInfo.getSizeSinceLastWrite());

        System.out.println("New Ledger Directory Path: " + newLedgerFile.getAbsolutePath());
        System.out.println("New Ledger Directory Writable: " + newLedgerFile.canWrite());

        long sizeSinceLastWrite = fileChannel.size() - 1024; // Dati dopo l'header
        System.out.println("Size since last write (calculated): " + sizeSinceLastWrite);

        // Assicurati che `sizeSinceLastWrite` non sia zero
        assertTrue("Size since last write should not be zero", sizeSinceLastWrite > 0);


        // Invoke the private method using reflection
        moveMethod.invoke(indexPersistenceMgr, 1345L, fileInfo);

        // Capture the file used in moveToNewLocation
//        ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
//        verify(fileInfo, times(1)).moveToNewLocation(fileCaptor.capture(), anyLong());

        // Verify the captured file is in the new directory
//        File capturedFile = fileCaptor.getValue();
//        assertNotNull("The new file should not be null", capturedFile);
//        assertEquals("The file should be moved to the new directory", newLedgerDir, capturedFile.getParentFile());

        // Verifica che il file sia stato spostato nella nuova directory
        assertTrue("The file should have been moved to the new  file", newLedgerFile.exists());

        try (RandomAccessFile raf = new RandomAccessFile(newLedgerFile, "r")) {
            raf.seek(1024);
            byte[] buffer = new byte[9]; // "test data".length()
            raf.read(buffer);
            String content = new String(buffer);
            assertEquals("test data", content);
        }

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
}
