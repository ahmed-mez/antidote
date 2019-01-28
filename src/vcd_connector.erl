-module(vcd_connector).
-export([init/0, recv_one_msg/1, send/3, close_socket/1]).

init() ->
    net_kernel:start([test, shortnames]),
    application:ensure_started(java_erlang),
    java:set_timeout(infinity),    
    {ok,NodeId} = java:start_node([{add_to_java_classpath,["../bin/vcd.jar"]}]), %% TODO put the vcd file path in some config file
    ZkIP = os:getenv("ZK", "-zk=127.0.0.1:2181"), %% default value is 127.0.0.1:2181 if ZK env var is not set, ZK should respect the format -zk=ip:port
    Conf = java:call_static(NodeId,'org.imdea.vcd.Config',parseArgs,[[ZkIP]]),
    Socket = java:call_static(NodeId,'org.imdea.vcd.Socket',create,[Conf,10]),
    {NodeId, Socket}.


recv_one_msg(Socket) ->
    RcvSet = java:call(Socket,'receive',[]),
    RcvMsg = java:call(java:call(RcvSet,getMessagesList,[]),get,[0]),
    Status = java:call(RcvSet, getStatusValue, []),
    {RcvMsg, Status}.

send(NodeId, Socket, MsgList) ->
    MgbMsgSetBuilder = java:call_static(NodeId,'org.imdea.vcd.pb.Proto.MessageSet', newBuilder,[]),
    MgbMsgStatus = java:call(MgbMsgSetBuilder, setStatusValue, [0]),
    MgbMsgAdded = java:call(MgbMsgStatus, addAllMessages, [MsgList]),
    MessageSet = java:call(MgbMsgAdded, build, []),
    java:call(Socket,send,[MessageSet]),
    SendMsg = java:call(java:call(MessageSet,getMessagesList,[]),get,[0]),
    SendMsg.

close_socket(Socket) -> 
    java:call(Socket, close, []).




