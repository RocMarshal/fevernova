package com.github.fevernova.framework.task;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.ComponentChangeMode;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.service.aligned.AlignService;
import com.github.fevernova.framework.service.barrier.BarrierGeneratorJob;
import com.github.fevernova.framework.service.barrier.BarrierService;
import com.github.fevernova.framework.service.monitor.MonitorJob;
import com.github.fevernova.framework.service.monitor.MonitorService;
import com.github.fevernova.framework.service.scheduler.Schedulerd;
import com.github.fevernova.framework.service.state.StateService;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Manager {


    private static Manager instance;

    protected final GlobalContext globalContext;

    protected final TaskContext taskContext;

    @Getter
    protected final BarrierService barrierService;

    @Getter
    protected final AlignService alignService;

    @Getter
    protected final MonitorService monitorService;

    @Getter
    protected final StateService stateService;

    @Getter
    protected final Schedulerd scheduler;

    protected TaskTopology taskTopology;


    private Manager(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.barrierService = new BarrierService(globalContext, new TaskContext(Constants.BARRIER, taskContext.getSubProperties(Constants.BARRIER_)));
        this.alignService = new AlignService(globalContext, new TaskContext(Constants.ALIGN, taskContext.getSubProperties(Constants.ALIGN_)));
        this.stateService = new StateService(globalContext, new TaskContext(Constants.STATE, taskContext.getSubProperties(Constants.STATE_)));
        this.monitorService = new MonitorService(globalContext, new TaskContext(Constants.MONITOR, taskContext.getSubProperties(Constants.MONITOR_)));
        this.scheduler = new Schedulerd(globalContext, new TaskContext(Constants.TIMER, taskContext.getSubProperties(Constants.TIMER_)));
    }


    public static Manager getInstance(GlobalContext globalContext, TaskContext taskContext) {

        if (instance == null) {
            synchronized (Manager.class) {
                if (instance == null)
                    instance = new Manager(globalContext, taskContext);
            }
        }
        return instance;
    }


    public static Manager getInstance() {

        return getInstance(null, null);
    }


    public void register(TaskTopology taskTopology) {

        log.info("Manager register . ");
        this.taskTopology = taskTopology;
        this.barrierService.register(taskTopology);
        this.monitorService.register(taskTopology);
    }


    public void recovery() {

        if (this.stateService.isSupportRecovery()) {
            this.taskTopology.recovery();
        }
    }


    public void execute() throws Exception {

        log.info("Manager execute . ");
        this.taskTopology.execute();
        this.scheduler.createJob(BarrierGeneratorJob.class, Schedulerd.SCHEDULER_BARRIER, 20, Maps.newHashMap());
        this.scheduler.createJob(MonitorJob.class, Schedulerd.SCHEDULER_MONITOR, 60, Maps.newHashMap());
        this.scheduler.start();
    }


    public void close() throws Exception {

        log.info("Manager close . ");
        this.scheduler.close();
        this.taskTopology.close();
    }


    public void updateTaskTopology(ComponentType component, ComponentChangeMode change) {

        log.info("Manager updateTaskTopology : {} will be {}" + component, change);
        try {
            this.scheduler.pauseJob(Schedulerd.SCHEDULER_BARRIER);
            this.taskTopology.sourcePause();
            //通过发送barrier的方式，等待下游消费完毕所有数据
            BarrierData barrierData = barrierService.generateBarrier();
            while (barrierService.isBarrierRunning(barrierData)) {
                Util.sleepMS(1);
            }
            this.taskTopology.change(component, change);
            this.taskTopology.sourceResume();
            this.scheduler.resumeJob(Schedulerd.SCHEDULER_BARRIER);
        } catch (Exception e) {
            this.globalContext.fatalError("Manager updateTaskTopology error : ", e);
        }
    }

}
