/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 */
public class WindowWordCountFS {

	public static final TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
		new String[]{
			"userId",
			"username",
			"url",
			"clickTime",
			"user_rank",
			"uuid",
			"date_str",
			"time_str",
			"final_state"
		},
		new TypeInformation[]{
			Types.STRING(),
			Types.STRING(),
			Types.STRING(),
			Types.SQL_TIMESTAMP(),
			Types.STRING(),
			Types.STRING(),
			Types.STRING(),
			Types.STRING(),
			Types.STRING()
		});

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.setMinPauseBetweenCheckpoints(4000);
		checkpointConfig.setCheckpointTimeout(1000 * 600);
		checkpointConfig.setMaxConcurrentCheckpoints(1);
		checkpointConfig.enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend((StateBackend) new FsStateBackend("hdfs:///tmp/shiy/flink-checkpoints/"));

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "192.168.2.170:9092");
			properties.setProperty("group.id", "shiy_topic_final_state_group_id_fs");
			properties.setProperty("enable.auto.commit", "false");
			FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("shiy_topic_final_state", new SimpleStringSchema(), properties);
			text = env.addSource(consumer010).setParallelism(1);
		} else {
			System.out.println("Executing WindowWordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final int windowSize = params.getInt("window", 10);
		final int slideSize = params.getInt("slide", 5);

		final Row row = new Row(9);

		SingleOutputStreamOperator<Row> splitStream = text.map(new RichMapFunction<String, Row>() {
			@Override
			public Row map(String value) throws Exception {
				String[] strings = value.split(",");
				for (int i = 0; i < strings.length; i++) {
					if (i == 3) {
						row.setField(i, Timestamp.valueOf(strings[i]));
					} else {
						row.setField(i, strings[i]);
					}
				}
				return row;
			}
		}).returns(USER_CLICK_TYPEINFO);

		SingleOutputStreamOperator sink = splitStream
			.keyBy("username")
			.process(new FinalStateMergeProcessFunction(
				(RowTypeInfo) USER_CLICK_TYPEINFO,
				300,
				false,
				"final_state",
				"FINAL_1,FINAL_2",
				","))
			.returns(USER_CLICK_TYPEINFO)
			.name("WaitMergeProcessFunction");

		String tumbleWindowSql = "select username, count(*) as cnt, " +
			"" +
			"TUMBLE_START(clickTime, INTERVAL '60' SECOND) as window_start, " +
			"TUMBLE_END(clickTime, INTERVAL '60' SECOND) as window_end " +
			"from urlclick_table " +
			"group by username, " +
			"TUMBLE(clickTime, INTERVAL '60' SECOND)";

		SingleOutputStreamOperator<Row> watermarks = sink.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
			@Override
			public long extractAscendingTimestamp(Row element) {
				return Timestamp.valueOf(element.getField(3).toString()).getTime();
			}
		});

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.registerDataStream("urlclick_table", watermarks,
			"userId,username,url,clickTime.rowtime,user_rank,uuid,date_str,time_str,final_state");

		Table table = tEnv.sqlQuery(tumbleWindowSql);
		DataStream<Row> sinkStream = tEnv.toAppendStream(table, Row.class);
		sinkStream.writeAsText("hdfs:///tmp/shiy/windows_word_count_out_fs/", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("WindowWordCountFileSystem_clear_" + System.currentTimeMillis());
	}
}
