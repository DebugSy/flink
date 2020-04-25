package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by P0007 on 2020/3/17.
 * 缓存数据waitSeconds秒，等待新到的数据与它合并，到达触发时间合并后往下发送.
 */
public class WaitMergeProcessFunction extends KeyedProcessFunction<Tuple, Row, Row> {

	private static final Logger log = LoggerFactory.getLogger(WaitMergeProcessFunction.class);

	private final int keyColumnIndex;

	private final RowTypeInfo rowTypeInfo;

	private final long waitSeconds;

	private transient MapState<Object, Long> keyMapState;

	private transient MapState<Long, List<Row>> mapState;

	private transient long mergedCnt = 0L;

	private transient long processedCnt = 0L;

	public WaitMergeProcessFunction(RowTypeInfo rowTypeInfo, String waitKeyColumn, long waitSeconds) {
		int keyFieldIndex = rowTypeInfo.getFieldIndex(waitKeyColumn);
		if (keyFieldIndex == -1) {
			throw new RuntimeException(waitKeyColumn + " do not exist in " + rowTypeInfo);
		}
		this.rowTypeInfo = rowTypeInfo;
		this.keyColumnIndex = keyFieldIndex;
		this.waitSeconds = waitSeconds;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
		TypeInformation<List<Row>> valueInfo = TypeInformation.of(new TypeHint<List<Row>>() {
		});
		MapStateDescriptor stateDescriptor = new MapStateDescriptor("buffer", keyInfo, valueInfo);
		this.mapState = getRuntimeContext().getMapState(stateDescriptor);

		TypeInformation<Object> keyMapKeyInfo = TypeInformation.of(Object.class);
		TypeInformation<Long> keyMapValueInfo = TypeInformation.of(Long.class);
		MapStateDescriptor keyMapDescriptor = new MapStateDescriptor("keyMap", keyMapKeyInfo, keyMapValueInfo);
		this.keyMapState = getRuntimeContext().getMapState(keyMapDescriptor);

		//metrics
		Gauge<Long> mergedCntGauge = getRuntimeContext()
			.getMetricGroup()
			.addGroup("custom_group")
			.gauge("mergedCnt", () -> mergedCnt);

		Gauge<Long> processedCntGauge = getRuntimeContext()
			.getMetricGroup()
			.addGroup("custom_group")
			.gauge("processedCnt", () -> processedCnt);
	}

	@Override
	public void processElement(Row newRow, Context ctx, Collector<Row> out) throws Exception {
		TimerService timerService = ctx.timerService();
		Long newEventTime = ctx.timestamp();
		/* 没有指定eventTime或者使用ProcessTime */
		if (newEventTime == null) {
			throw new RuntimeException("Please assign timestamps,watermarks and use EventTime characteristic");
		}
		Object keyFieldValue = newRow.getField(keyColumnIndex);
		if (keyMapState.contains(keyFieldValue)) {
			//merge all rows
			Long registerTime = keyMapState.get(keyFieldValue);
			if (registerTime < newEventTime) {
				log.debug("registerTime < newEventTime: {} < {},", registerTime, newEventTime);
				processNoBufferRow(newRow, timerService, newEventTime, keyFieldValue);
			} else {
				long rowEventTime = registerTime - 1000 * waitSeconds;
				boolean late = newEventTime < rowEventTime;
				List<Row> rows = mapState.get(registerTime);
				Iterator<Row> iterator = rows.iterator();
				while (iterator.hasNext()) {
					Row row = iterator.next();
					Object fieldValue = row.getField(keyColumnIndex);
					if (keyFieldValue.equals(fieldValue)) {
						log.debug("Merging row :\n{}\n{}", newRow, row);
						if (newRow.getArity() != row.getArity()) {
							throw new RuntimeException("Row arity not equal");
						} else {
							for (int i = 0; i < newRow.getArity(); i++) {
								Object valueFieldV = newRow.getField(i);
								Object rowFieldV = row.getField(i);
								String type = rowTypeInfo.getTypeAt(i).toString();
								if (late) {
									row.setField(i, StringUtils.isNullOrWhitespaceOnly(rowFieldV.toString()) ? valueFieldV : rowFieldV);
								} else {
									row.setField(i, StringUtils.isNullOrWhitespaceOnly(valueFieldV.toString()) ? rowFieldV : valueFieldV);
								}
							}
						}
						mergedCnt++;
						log.debug("Merged result {}", row);
					}
				}
				mapState.put(registerTime, rows);
			}
		} else {
			log.debug("No such key {} exists in the buffer", keyFieldValue);
			processNoBufferRow(newRow, timerService, newEventTime, keyFieldValue);
		}
		processedCnt++;
	}

	/**
	 * 处理新到的数据，注册Timer等待触发
	 *
	 * @param newRow        新到的数据
	 * @param timerService  timer服务
	 * @param eventTime     事件时间
	 * @param keyFieldValue key字段的值
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private void processNoBufferRow(Row newRow, TimerService timerService, Long eventTime, Object keyFieldValue) throws Exception {
		List<Row> rows = mapState.get(eventTime);
		rows = rows == null ? new ArrayList<>() : rows;
		rows.add(newRow);
		/*
		 * 注册waitSeconds秒后的Timer，以触发向下发送数据
		 * */
		long registerTime = eventTime + 1000 * waitSeconds;
		keyMapState.put(keyFieldValue, registerTime);
		mapState.put(registerTime, rows);
		log.debug("Register timer @ {}, key is {}", registerTime, keyFieldValue);
		timerService.registerEventTimeTimer(registerTime);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
		log.debug("On Timer @ {}", timestamp);
		TimerService timerService = ctx.timerService();
		List<Row> rows = mapState.get(timestamp);
		Iterator<Row> iterator = rows.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			out.collect(row); // collect row
			Object keyField = row.getField(keyColumnIndex);
			log.debug("On Timer @ {}, remove key {}, collect row {}", timestamp, keyField, row);
			keyMapState.remove(keyField);
		}
		mapState.remove(timestamp);
		timerService.deleteEventTimeTimer(timestamp);
	}
}
