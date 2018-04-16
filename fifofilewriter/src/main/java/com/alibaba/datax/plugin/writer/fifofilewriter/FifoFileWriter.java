package com.alibaba.datax.plugin.writer.fifofilewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FifoFileWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final String FIFO_TEMPLATE = "${fifo}";

        private Configuration writerSliceConfig;
        private final String jobName;
        private int fifoCount;
        private List<FIFO> fifos = new ArrayList<FIFO>();
        private BlockingQueue<FIFO> fifoQueue = new LinkedBlockingQueue<FIFO>();

        public Job() {
            this.jobName = UUID.randomUUID().toString();
            jobMap.put(this.jobName, this);
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void prepare() {
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            jobMap.remove(this.jobName);

            // Close all fifos.
            for (FIFO fifo: fifos) {
                try {
                    fifo.close();
                } catch (Exception e) {
                    LOG.warn("Could not close fifo", fifo);
                }
            }
        }

        private void configureFifos(int fifoCount) {
            // Get the buffer size; default is 768K
            int bufferSize = this.writerSliceConfig.getInt(Key.BUFFER_SIZE, 768*1024);

            // Validate directory.
            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH, FifoFileWriterErrorCode.REQUIRED_VALUE);
            try {
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException.asDataXException(FifoFileWriterErrorCode.ILLEGAL_VALUE, path);
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException.asDataXException(FifoFileWriterErrorCode.CONFIG_INVALID_EXCEPTION, path);
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.SECURITY_NOT_ENOUGH, path, se);
            }

            // Validate write mode.
            String writeMode = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            if ("nonConflict".equals(writeMode)) {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.CONFIG_INVALID_EXCEPTION, "nonConflict");
            } else if ("truncate".equals(writeMode)) {
                String fileName = this.writerSliceConfig.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME, FifoFileWriterErrorCode.REQUIRED_VALUE);

                // If configured to delete on start, do that now.
                if ("true".equals(this.writerSliceConfig.getString(Key.DELETE_ON_START))) {
                    for (File fifoFile: getFifoFiles(path)) {
                        try {
                            LOG.info("Deleting fifo: [{}]", fifoFile);
                            FileUtils.forceDelete(fifoFile);
                        } catch (IOException e) {
                            throw DataXException.asDataXException(FifoFileWriterErrorCode.REMOVE_FIFO_ERROR, fifoFile.toString(), e);
                        }
                    }
                }

                // Create fifos.
                for (int i = 0; i < fifoCount; ++i) {

                    // If no template provided, just append #
                    String fifoName;
                    if (fileName.contains(FIFO_TEMPLATE)) {
                        fifoName = fileName.replace(FIFO_TEMPLATE, Integer.toString(i));
                    } else {
                        fifoName = String.format("%s__%d", fileName, i);
                    }

                    // Create the fifo if it does not exist.
                    File fifoFile = new File(path, fifoName);
                    if (!fifoFile.exists()) {
                        try {
                            LOG.info("Creating fifo: [{}]", fifoFile);
                            Runtime.getRuntime().exec(new String[]{"sh", "-c", "mkfifo " + fifoFile.getAbsolutePath()});
                        } catch (IOException e) {
                            throw DataXException.asDataXException(FifoFileWriterErrorCode.CREATE_FIFO_ERROR, fifoFile.toString(), e);
                        }
                    } else {
                        LOG.info("Using existing fifo: [{}]", fifoFile);
                    }

                    FIFO fifo = new FIFO(fifoFile, bufferSize);
                    fifos.add(fifo);
                    fifoQueue.add(fifo);
                }
            } else if ("append".equals(writeMode)) {

                for (File fifoFile: getFifoFiles(path)) {
                    LOG.info("Using existing fifo: [{}]", fifoFile);
                    FIFO fifo = new FIFO(fifoFile, bufferSize);
                    fifos.add(fifo);
                    fifoQueue.add(fifo);
                }
            } else {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.CONFIG_INVALID_EXCEPTION, writeMode);
            }

            // Open the fifos now, unless told otherwise.
            if (this.writerSliceConfig.getBool(Key.OPEN_FIFOS_ON_INIT, true)) {
                for (FIFO fifo : fifos) {
                    fifo.open();
                }
            }
        }

        private File[] getFifoFiles(String path) {
            return new File(path).listFiles(new FileFilter() {
                            @Override
                            public boolean accept(File file) {
                                return !file.isFile() && !file.isDirectory();
                            }
                        });
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // Configure the # of fifos, or allocate as needed.
            configureFifos(mandatoryNumber);

            LOG.info("Begin split");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitConfiguration = this.writerSliceConfig.clone();
                splitConfiguration.set(Key.JOB, this.jobName);
                writerSplitConfigs.add(splitConfiguration);
            }
            LOG.info("End split");
            return writerSplitConfigs;
        }

        FIFO allocateFifo() {
            try {
                return fifoQueue.take().open();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void returnFifo(FIFO fifo) {
            fifoQueue.add(fifo.end());
        }
    }

    private static class FIFO {
        private static final Logger LOG = LoggerFactory.getLogger(FIFO.class);
        private final File fifoFile;
        private final int bufferSize;
        private OutputStream targetStream;
        private OutputStream fifoStream;
        private boolean allocated;

        private FIFO(File fifoFile, int bufferSize) {
            this.fifoFile = fifoFile;
            this.bufferSize = bufferSize;
        }

        FIFO close() {
            if (!this.allocated) {
                // TODO: open then close can hang, so spawn a thread to do this and wait 1-5s.
                open();
            }
            if (this.targetStream != null) {
                IOUtils.closeQuietly(this.targetStream);
            }
            this.targetStream = null;
            this.fifoStream = null;
            return this;
        }

        FIFO open() {
            if (this.fifoStream == null) {
                try {
                    LOG.info("Opening fifo: {}", this);
                    this.targetStream = new BufferedOutputStream(new FileOutputStream(fifoFile), this.bufferSize);
                    this.fifoStream = new FilterOutputStream(targetStream) {
                        @Override
                        public void close() throws IOException {
                            // Don't let this happen until we are all done.
                            // super.close();
                        }
                    };
                } catch (FileNotFoundException e) {
                    throw DataXException.asDataXException(FifoFileWriterErrorCode.OPEN_FIFO_ERROR, fifoFile.toString(), e);
                }
            }

            return this;
        }

        FIFO end() {
            try {
                this.targetStream.write('\n');
                this.targetStream.flush();
            } catch (IOException e) {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.WRITE_FILE_IO_ERROR, fifoFile.toString(), e);
            }
            return this;
        }

        FIFO allocated() {
            this.allocated = true;
            return this;
        }

        @Override
        public String toString() {
            return this.fifoFile.toString();
        }
    }

    private static Map<String, Job> jobMap = Collections.synchronizedMap(new HashMap<String, Job>());

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
        private Job job;
        private FIFO fifo;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.job = jobMap.get(this.writerSliceConfig.getString(Key.JOB));
            this.fifo = this.job.allocateFifo();
            LOG.info("Allocated fifo: [{}]", this.fifo);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            // Is duplication configured?
            final int duplicateRecordCount = this.writerSliceConfig.getInt(Key.DUPLICATE_RECORD_COUNT, 0);
            if (duplicateRecordCount > 0) {
                final RecordReceiver delegateLineReceiver = lineReceiver;
                RecordReceiver duplicatingLineReceiver = new RecordReceiver() {
                    int current;
                    Record currentRecord;

                    @Override
                    public Record getFromReader() {
                        if (current == 0) {
                            current = duplicateRecordCount;
                            currentRecord = delegateLineReceiver.getFromReader();
                        }
                        --current;
                        return currentRecord;
                    }

                    @Override
                    public void shutdown() {
                        delegateLineReceiver.shutdown();
                    }
                };
                lineReceiver = duplicatingLineReceiver;
            }

            LOG.info("Begin write to fifo: [{}]", this.fifo);
            try {
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver, this.fifo.fifoStream, this.writerSliceConfig, this.fifo.toString(), this.getTaskPluginCollector());
            } catch (SecurityException se) {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.SECURITY_NOT_ENOUGH, this.fifo.toString());
            } catch (Exception e) {
                throw DataXException.asDataXException(FifoFileWriterErrorCode.WRITE_FILE_IO_ERROR, this.fifo.toString(), e);
            }
            LOG.info("End write to fifo: [{}]", this.fifo);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            this.job.returnFifo(this.fifo);
        }
    }
}
