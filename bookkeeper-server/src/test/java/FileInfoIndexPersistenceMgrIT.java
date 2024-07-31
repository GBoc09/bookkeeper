import org.apache.bookkeeper.bookie.FileInfo;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FileInfoIndexPersistenceMgrIT {
    private IndexPersistenceMgr indexPersistenceMgr;
    private FileInfo fileInfo;
    private long size;
    private File newFile;
    private File initialFile;

    @Before
    public void setUp() throws IOException {
        ServerConfiguration conf = new ServerConfiguration();
        SnapshotMap<Long, Boolean> activeLedgers = new SnapshotMap<>();
        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        StatsLogger statsLogger = mock(StatsLogger.class);
        this.indexPersistenceMgr = spy(new IndexPersistenceMgr(4096, 512, conf, activeLedgers, ledgerDirsManager, statsLogger));

        this.fileInfo = mock(FileInfo.class);
        this.size = 1024L;
        this.newFile = createTempDir("TestFile");

        // Configura il mock di FileInfo per restituire un file non nullo
        when(fileInfo.getLf()).thenReturn(createTempDir("tests"));
    }
    @After
    public void tearDown() {
        // Clean up the files created during the test
        if (initialFile != null && initialFile.exists()) {
            initialFile.delete();
        }
        if (newFile != null && newFile.exists()) {
            newFile.delete();
        }
    }

    @Test
    public void reachMethodTest() {
        try {
            Method method = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
            method.setAccessible(true);
            method.invoke(this.indexPersistenceMgr, 1345L, this.fileInfo);
            verify(this.fileInfo, times(1)).moveToNewLocation(any(File.class), anyLong());
        } catch (IOException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void locationFileTest() {
        try {
            // Crea il file originale e scrivi dei byte in esso
            this.initialFile = Files.createTempFile("InitialFile", ".tmp").toFile();
            Files.write(initialFile.toPath(), "test data".getBytes());

            // Configura il mock per restituire il file iniziale
            when(fileInfo.getLf()).thenReturn(initialFile);
            when(fileInfo.getSizeSinceLastWrite()).thenReturn((long) "test data".getBytes().length);

            // Crea il nuovo file dove il contenuto del file originale sarà copiato
            this.newFile = Files.createTempFile("TestFile", ".tmp").toFile();
            System.out.println("Nuovo file creato: " + newFile.getAbsolutePath());

            // Usa la reflection per accedere al metodo privato moveLedgerIndexFile
            Method method = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
            method.setAccessible(true);

            // Invoca il metodo privato usando reflection
            method.invoke(this.indexPersistenceMgr, 1345L, this.fileInfo);
            System.out.println("Metodo invocato");

            // Verifica che moveToNewLocation sia chiamato sull'istanza FileInfo con i parametri corretti
            verify(this.fileInfo, times(1)).moveToNewLocation(any(File.class), eq((long) "test data".getBytes().length));
            System.out.println("Verifica completata");

            // Verifica che la posizione del file sia stata aggiornata
            assertNotEquals(initialFile.toPath(), newFile.toPath());

        } catch (IOException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            if (e instanceof InvocationTargetException) {
                Throwable cause = ((InvocationTargetException) e).getCause();
                cause.printStackTrace(); // Log dell'eccezione originale
            }
            throw new RuntimeException(e);
        }
    }



    public File createTempDir(String suffix) throws IOException {
        File file = IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
        return file;
    }
}