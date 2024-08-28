package org.apche.bookkeeper.IT;

import org.apache.bookkeeper.bookie.FileInfo;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

public class FileInfoPersistanceRelocationIT {
    private IndexPersistenceMgr indexPersistenceMgr;
    private FileInfo fileInfo;
    private FileInfo newFileInfo;
    private FileInfo spyFileInfo;
    private File currentFile;
    private File newLedgerFile;
    private LedgerDirsManager ledgerDirsManager;
    private ServerConfiguration serverConfiguration = new ServerConfiguration();
    private FileChannel fileChannel;
    private String masterKey = "masterKey";
    private Path tempDirectory;
    private File accessDir;
    private File accessFile;


    @Before
    public void setUp() throws Exception {
        System.out.println("SET UP method:");

        // Configure LedgerDirsManager mock
        ledgerDirsManager = mock(LedgerDirsManager.class);
        // Creazione della directory principale
        Path baseDirectory = Files.createTempDirectory("ledgerBaseDir");
        Path subDirectory = Files.createDirectories(baseDirectory.resolve("subDir"));
        Path furtherSubDirectory = Files.createDirectories(subDirectory.resolve("furtherSubDir"));
        Path tempFile = Files.createTempFile(baseDirectory, "MockFile", ".tmp");

        currentFile = tempFile.toFile();
        System.out.println("currentFile: " + currentFile);
        accessDir = currentFile.getParentFile();
        System.out.println("accessDir: " + accessDir.getAbsolutePath());
//        tempDirectory = Files.createTempFile(baseDirectory, "MockFile", ".tmp");
//        accessFile = tempDirectory.toFile();
//        System.out.println("accessFile: " + accessFile.getAbsolutePath());
        // Write a valid header to the file
        try (RandomAccessFile raf = new RandomAccessFile(currentFile, "rw")) {
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

        fileChannel = new RandomAccessFile(currentFile, "rw").getChannel();
        // Create instance of IndexPersistenceMgr
        indexPersistenceMgr = new IndexPersistenceMgr(
                4096,
                512,
                serverConfiguration,
                new SnapshotMap<>(),
                ledgerDirsManager,
                mock(StatsLogger.class)
        );
        System.out.println("indexPersistenceMgr instance: " + indexPersistenceMgr);

        // real FileInfo object
        fileInfo = new FileInfo(currentFile, masterKey.getBytes(), 0);

        // Mock FileInfo with spy to track method interactions
        spyFileInfo = spy(fileInfo);

        // Set the FileChannel directly via reflection to ensure it is initialized
        Field fcField = FileInfo.class.getDeclaredField("fc");
        fcField.setAccessible(true);
        fcField.set(spyFileInfo, fileChannel);
        assertTrue("FileChannel should be open", fileChannel.isOpen());

        // Posiziona il puntatore del canale dopo l'header
        fileChannel.position(1024);
        ByteBuffer byteBuffer[] = new ByteBuffer[1];
        byteBuffer[0] = ByteBuffer.wrap("testData".getBytes());
        spyFileInfo.write(byteBuffer, 0);
        fileChannel.write(byteBuffer);

        // Sincronizza il canale per assicurarsi che i dati siano effettivamente scritti
        fileChannel.force(true);
        ByteBuffer verifyBuffer = ByteBuffer.allocate(8); // "testData".length()
        fileChannel.position(1024); // Reimposta la posizione per la lettura
        fileChannel.read(verifyBuffer);
        verifyBuffer.flip();
        String writtenContent = new String(verifyBuffer.array());
        System.out.println("Data written to file: " + writtenContent);

        ByteBuffer buffer = ByteBuffer.allocate(1033);
        spyFileInfo.read(buffer, 0, true);
        for (int i = 0; i < 8; i++) {
            System.out.println("string in fileInfo: " + (char) buffer.array()[i]);
        }

        System.out.println("sincronized data: " + spyFileInfo.getSizeSinceLastWrite());

    }

    @After
    public void tearDown() throws Exception {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
        if (currentFile != null && currentFile.exists()) {
            currentFile.delete();
        }
    }

    @Test
    public void relocateIndexAndFlushHeaderTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, NoSuchFieldException {
        System.out.println("TEST METHOD");
        accessDir = currentFile.getParentFile().getParentFile().getParentFile();
        System.out.println("accessDir: "+accessDir.getAbsolutePath());
        when(ledgerDirsManager.isDirFull(accessDir)).thenReturn(true);
        Method relecateIndexMethod = IndexPersistenceMgr.class.getDeclaredMethod("relocateIndexFileAndFlushHeader", long.class, FileInfo.class);
        relecateIndexMethod.setAccessible(true);
        relecateIndexMethod.invoke(indexPersistenceMgr, 1024L, spyFileInfo);

        verify(spyFileInfo, times(1)).moveToNewLocation(any(File.class), anyLong());
        verify(spyFileInfo, times(1)).flushHeader();
    }
}
