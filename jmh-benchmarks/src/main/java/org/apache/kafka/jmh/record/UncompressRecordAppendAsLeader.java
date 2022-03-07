package org.apache.kafka.jmh.record;

import kafka.api.ApiVersion;
import kafka.log.*;
import kafka.log.Defaults;
import kafka.server.*;
import kafka.utils.KafkaScheduler;
import kafka.utils.MockTime;
import kafka.utils.Scheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.*;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class UncompressRecordAppendAsLeader extends BaseRecordBatchBenchmark{


    private File logDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
    private Scheduler scheduler; // = new KafkaScheduler(1, "scheduler", true);
    //private KafkaScheduler scheduler = time.scheduler();

    private MockTime  time = new MockTime();

    private Log log ;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (!logDir.mkdir())
            throw new IOException("error creating test directory");

        //scheduler.startup();
        scheduler=time.scheduler();

       /* Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:9999");
        KafkaConfig config = new KafkaConfig(props);*/
        //LogConfig logConfig = createLogConfig();

       /* List<File> logDirs = Collections.singletonList(logDir);
        BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
        logManager = new LogManager(JavaConverters.asScalaIteratorConverter(logDirs.iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(new ArrayList<File>().iterator()).asScala().toSeq(),
                new CachedConfigRepository(),
                logConfig,
                new CleanerConfig(0, 0, 0, 0, 0, 0.0, 0, false, "MD5"),
                1,
                1000L,
                10000L,
                10000L,
                1000L,
                60000,
                scheduler,
                brokerTopicStats,
                logDirFailureChannel,
                Time.SYSTEM,
                true);*/


    }

    @Override
    CompressionType compressionType() {
        return CompressionType.NONE;
    }

    @TearDown
    public void close(){
        if (log != null){
            log.close();
        }
    }

    private static LogConfig createLogConfig() {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentMsProp(), Defaults.SegmentMs());
        logProps.put(LogConfig.SegmentBytesProp(), Defaults.SegmentSize());
        logProps.put(LogConfig.RetentionMsProp(), Defaults.RetentionMs());
        logProps.put(LogConfig.RetentionBytesProp(), Defaults.RetentionSize());
        logProps.put(LogConfig.SegmentJitterMsProp(), Defaults.SegmentJitterMs());
        logProps.put(LogConfig.CleanupPolicyProp(), Defaults.CleanupPolicy());
        logProps.put(LogConfig.MaxMessageBytesProp(), Defaults.MaxMessageSize());
        logProps.put(LogConfig.IndexIntervalBytesProp(), Defaults.IndexInterval());
        logProps.put(LogConfig.SegmentIndexBytesProp(), Defaults.MaxIndexSize());
        logProps.put(LogConfig.MessageFormatVersionProp(), Defaults.MessageFormatVersion());
        logProps.put(LogConfig.FileDeleteDelayMsProp(), Defaults.FileDeleteDelayMs());
        return LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
    }

    @Benchmark
    public void measureAnalyzeAndValidateRecords(Blackhole bh) {
        MemoryRecords records = MemoryRecords.readableRecords(singleBatchBuffer.duplicate());
        Iterator<MutableRecordBatch> it = records.batches().iterator();
        while (it.hasNext()){
            it.next().isValid();
        }
        TopicPartition topicAndPartition = new TopicPartition("key", 0);
        log = new Log(logDir,
                createLogConfig(),
                0L,
                0L,
                time.scheduler(),
                brokerTopicStats,
                time,
                60 * 60 * 1000,
                LogManager.ProducerIdExpirationCheckIntervalMs(),
                topicAndPartition,
                new ProducerStateManager(topicAndPartition,logDir,60 * 60 * 1000),
                new LogDirFailureChannel(10),
                true,
                true);
        log.appendAsLeader(records,0, new AppendOrigin.Client$(), ApiVersion.latestVersion());
    }
}

