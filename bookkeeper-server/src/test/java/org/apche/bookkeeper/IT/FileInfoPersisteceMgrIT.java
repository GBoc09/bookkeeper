package org.apche.bookkeeper.IT;

import org.apache.bookkeeper.bookie.FileInfo;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FileInfoPersisteceMgrIT {

    private IndexPersistenceMgr indexPersistenceMgr;
    private FileInfo fileInfo;
    private File initialFile;
    private File newLedgerDir;
    private LedgerDirsManager ledgerDirsManager;

    @Before
    public void setUp() throws Exception {
        // Create temporary directory and file
        Path tempDirectory = Files.createTempDirectory("ledgerDir");
        initialFile = Files.createTempFile(tempDirectory, "InitialFile", ".tmp").toFile();

        // Configure LedgerDirsManager mock
        ledgerDirsManager = mock(LedgerDirsManager.class);

        // Create instance of IndexPersistenceMgr
        indexPersistenceMgr = new IndexPersistenceMgr(
                4096,
                512,
                new ServerConfiguration(),
                new SnapshotMap<>(),
                ledgerDirsManager,
                mock(StatsLogger.class)
        );

        // Mock FileInfo with spy to track method interactions
        fileInfo = spy(new FileInfo(initialFile, "master-key".getBytes(), FileInfo.CURRENT_HEADER_VERSION));

        // Create a new directory for moving the file
        newLedgerDir = Files.createTempDirectory("newLedgerDir").toFile();

        // Mock behavior for picking a writable directory
        when(ledgerDirsManager.pickRandomWritableDirForNewIndexFile(initialFile.getParentFile())).thenReturn(newLedgerDir);
    }

    @After
    public void tearDown() throws Exception {
        if (initialFile != null && initialFile.exists()) {
            initialFile.delete();
        }
        if (newLedgerDir != null && newLedgerDir.exists()) {
            newLedgerDir.delete();
        }
    }

    @Test
    public void testMoveLedgerIndexFileInteraction() throws Exception {
        // Use reflection to access the private moveLedgerIndexFile method
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Ensure initial file exists
        assertTrue("The initial file should exist before the test.", initialFile.exists());

        // Invoke the private method using reflection
        moveMethod.invoke(indexPersistenceMgr, 1345L, fileInfo);

        // Capture the file used in moveToNewLocation
        ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
        verify(fileInfo, times(1)).moveToNewLocation(fileCaptor.capture(), anyLong());

        // Verify the captured file is in the new directory
        File capturedFile = fileCaptor.getValue();
        assertNotNull("The new file should not be null", capturedFile);
        assertEquals("The file should be moved to the new directory", newLedgerDir, capturedFile.getParentFile());
    }

    @Test
    public void testFileInfoSizeUpdate() throws Exception {
        // Mock the size returned by FileInfo
        doReturn(2048L).when(fileInfo).getSizeSinceLastWrite();

        // Use reflection to invoke moveLedgerIndexFile
        Method moveMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveMethod.setAccessible(true);

        // Invoke the method
        moveMethod.invoke(indexPersistenceMgr, 1345L, fileInfo);

        // Verify that moveToNewLocation was called with the correct size
        verify(fileInfo, times(1)).moveToNewLocation(any(File.class), eq(2048L));
    }
}
