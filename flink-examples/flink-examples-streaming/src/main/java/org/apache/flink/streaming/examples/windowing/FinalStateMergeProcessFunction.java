package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by P0007 on 2020/4/23.
 * 最终状态或者缓存数据waitSeconds秒数触发据合并
 */
public class FinalStateMergeProcessFunction extends KeyedProcessFunction<Tuple, Row, Row> {

	private static final Logger log = LoggerFactory.getLogger(FinalStateMergeProcessFunction.class);

    private final RowTypeInfo rowTypeInfo;

    private final long waitSeconds;

    private final int finalStateColIndex;

    private final List finalStates;

    private final boolean discard;

    private transient ValueState<Long> registerTimeState;

    private transient MapState<Long, Row> mapState;

    private transient long mergedCnt = 0L;

    private transient long processedCnt = 0L;

    private transient long finalStateCnt = 0L;

    private transient long noFinalStateCnt = 0L;

    private transient long mapStateSize = 0L;

    public FinalStateMergeProcessFunction(RowTypeInfo rowTypeInfo, long waitSeconds, boolean discard,
                                          String finalStateCol, String finalStatesStr, String finalStateSeparator) {
        this.rowTypeInfo = rowTypeInfo;
        this.waitSeconds = waitSeconds;
        this.finalStateColIndex = rowTypeInfo.getFieldIndex(finalStateCol);
        this.finalStates = new ArrayList();
        this.discard = discard;
        parserFinalStateValues(rowTypeInfo, finalStateCol, finalStatesStr, finalStateSeparator);
    }

    /**
     * 解析finalStateValue
     *
     * @param rowTypeInfo
     * @param finalStateCol       终态字段名称
     * @param finalStatesStr      终态字段值，可是多值，需要用 finalStateSeparator 分开
     * @param finalStateSeparator 终态字段值分隔符
     */
    private void parserFinalStateValues(RowTypeInfo rowTypeInfo, String finalStateCol, String finalStatesStr, String finalStateSeparator) {
        try {
            String[] finalStates = finalStatesStr.split(finalStateSeparator);
            String type = rowTypeInfo.getTypeAt(finalStateCol).toString();
            for (String finalState : finalStates) {
                Object convertdValue = ClassUtil.convert(finalState, type);
                this.finalStates.add(convertdValue);
            }
            log.debug("Parsed final state value is {}", this.finalStates);
        } catch (Exception e) {
            throw new RuntimeException("Parser final state values throw exception", e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
        TypeInformation<Row> valueInfo = TypeInformation.of(Row.class);
        MapStateDescriptor stateDescriptor = new MapStateDescriptor("buffer", keyInfo, valueInfo);
        this.mapState = getRuntimeContext().getMapState(stateDescriptor);

        TypeInformation<Long> registerTimeStateInfo = TypeInformation.of(Long.class);
        ValueStateDescriptor<Long> registerTimeStateDesc = new ValueStateDescriptor<>(
                "registerTime", registerTimeStateInfo);
        this.registerTimeState = getRuntimeContext().getState(registerTimeStateDesc);

        /*
        * 检查终态字段是否存在
        * */
        if (finalStateColIndex == -1) {
            throw new RuntimeException("Final state column not exist in " + rowTypeInfo);
        }

        //metrics
        Gauge<Long> mergedCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("mergedCnt", () -> mergedCnt);

        Gauge<Long> processedCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("processedCnt", () -> processedCnt);

        Gauge<Long> finalStateCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("finalStateCnt", () -> finalStateCnt);

        Gauge<Long> noFinalStateCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("noFinalStateCnt", () -> noFinalStateCnt);

        Gauge<Long> mapStateSizeGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("mapStateSize", () -> mapStateSize);
    }

    @Override
    public void processElement(Row newRow, Context ctx, Collector<Row> out) throws Exception {
        TimerService timerService = ctx.timerService();
        Long newEventTime = ctx.timestamp();
        /* 没有指定eventTime或者使用ProcessTime */
        if (newEventTime == null) {
            throw new RuntimeException("Please assign timestamps,watermarks.");
        }
        if (registerTimeState.value() == null) {
            registerTimeState.update(Long.MIN_VALUE);
        }
        Long registerTime = registerTimeState.value();
        if (registerTime != Long.MIN_VALUE) {
            //merge all rows
            if (registerTime < newEventTime) {
                log.debug("registerTime < newEventTime: {} < {},", registerTime, newEventTime);
                processNoBufferRow(newRow, timerService, newEventTime, ctx.getCurrentKey());
            } else {
                long rowEventTime = registerTime - 1000 * waitSeconds;
                boolean late = newEventTime < rowEventTime;
                Row row = mapState.get(registerTime);
				if (row == null || newRow == null) {
					log.error("row is empty. registerTime is {}.\n{}\n{}", registerTime, row, newRow);
				}
                log.debug("Merging row :\n{}\n{}", newRow, row);
                if (newRow.getArity() != row.getArity()) {
                    throw new RuntimeException("Row arity not equal");
                } else {
                    for (int i = 0; i < newRow.getArity(); i++) {
                        Object valueFieldV = newRow.getField(i);
                        Object rowFieldV = row.getField(i);
                        String type = rowTypeInfo.getTypeAt(i).toString();
                        if (late) {
                            row.setField(i, ClassUtil.isEmpty(rowFieldV, type) ? valueFieldV : rowFieldV);
                        } else {
                            row.setField(i, ClassUtil.isEmpty(valueFieldV, type) ? rowFieldV : valueFieldV);
                        }
                    }
                }
                mergedCnt++;
                log.debug("Merged result {}", row);

                /*
                 * 如果是终态则out.collect，否则更新mapState
                 * */
                Object finalState = row.getField(finalStateColIndex);
                if (finalStates.contains(finalState)) {
                    log.debug("On final state @ {}, registerTime {}, collect row {}", finalState, registerTime, row);
                    out.collect(row);
                    mapState.remove(registerTime);
                    timerService.deleteEventTimeTimer(registerTime);
                    mapStateSize--;
                    finalStateCnt++;
                } else {
                    mapState.put(registerTime, row);
                }
            }
        } else {
            log.debug("No such key {} exists in the buffer", ctx.getCurrentKey());
            processNoBufferRow(newRow, timerService, newEventTime, ctx.getCurrentKey());
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
        /*
         * 注册waitSeconds秒后的Timer，以触发向下发送数据
         * */
        Long registerTime = eventTime + 1000 * waitSeconds;
        mapState.put(registerTime, newRow);
        mapStateSize++;
        registerTimeState.update(registerTime);
        timerService.registerEventTimeTimer(registerTime);
        log.debug("Register timer @ {}, key is {}", registerTime, keyFieldValue);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        TimerService timerService = ctx.timerService();
        Row row = mapState.get(timestamp);
        if (row == null) {
            log.error("row is empty. On Timer @ {}", timestamp);
            return;
        }
        log.debug("On Timer @ {}, collect row {}", timestamp, row);
        /*
        * 超时数据
        * */
        if (!discard) {
            out.collect(row);
        }
        noFinalStateCnt++;
        mapState.remove(timestamp);
        mapStateSize--;
        registerTimeState.update(Long.MIN_VALUE);
        timerService.deleteEventTimeTimer(timestamp);
    }
}
