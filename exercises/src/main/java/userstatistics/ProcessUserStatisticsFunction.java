package userstatistics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import models.UserStatistics;

public class ProcessUserStatisticsFunction
        extends ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow> {
    private ValueStateDescriptor<UserStatistics> stateDescriptor;

    @Override
    public void open(Configuration config) throws Exception{
        stateDescriptor = new ValueStateDescriptor<UserStatistics>(
                "User Statistics",
                UserStatistics.class);
        super.open(config);
    }

    @Override
    public void process(
            String key, ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow>.Context context,
            Iterable<UserStatistics> listWindowStats, Collector<UserStatistics> collector) throws Exception {
        // 1. obtain current state
        ValueState<UserStatistics> state = context.globalState().getState(stateDescriptor);
        // 2. extract value, make sure we handle initial condition of object being null
        UserStatistics userStatistics = state.value();
        // 3. process list
        for (UserStatistics ws: listWindowStats){
            if (userStatistics == null)
                userStatistics = ws;
            else 
                userStatistics = userStatistics.merge(ws);
        }
        // 4. carry newly computed state to next window
        state.update(userStatistics);
        // 5. emit newly computed state into current output stream
        collector.collect(userStatistics);

    }

}
