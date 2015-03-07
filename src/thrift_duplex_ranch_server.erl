%%%-------------------------------------------------------------------
%%% @author davidalphafox <>
%%% @copyright (C) 2015, davidalphafox
%%% @doc
%%%
%%% @end
%%% Created :  7 Mar 2015 by davidalphafox <>
%%%-------------------------------------------------------------------
-module(thrift_duplex_ranch_server).

-behaviour(gen_server2).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
		transport,
		transport_ok,
		transport_closed,
		transport_error,
		socket,
		read_buffer
	}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ListenerPid, Socket, Transport, Opts) ->
	{ok,Pid} = gen_server2:start_link(?MODULE, [], []),
	set_socket(Pid,ListenerPid,Socket,Transport,Opts),
	{ok,Pid}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
	State = #state{
		read_buffer = <<>>
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
	Reply = ok,
	{reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({socket_ready,ListenerPid, Socket,Transport},State)->
	ranch:accept_ack(ListenerPid),
	ok = Transport:setopts(Socket, [{active, once}, binary]),
   	{OK, Closed, Error} = Transport:messages(),
	NewState = State#state{
		transport = Transport,
		transport_ok = OK,
		transport_closed = Closed,
		transport_error = Error,
		socket = Socket
	},
	{noreply,NewState};

handle_cast(_Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({OK, Socket, Bin},#state{transport_ok = OK,socket = Socket} = State) ->
	Transport = State#state.transport,
	ReadBuffer = State#state.read_buffer,
  	% Flow control: enable forwarding of next TCP message
  	ok = Transport:setopts(Socket, [{active, false}]),
  	{Framed,NewReadBuffer} = unpack(<<ReadBuffer/bits,Bin/bits>>,[]),
  	ok = Transport:setopts(Socket, [{active, once}]),
 	NewState = State#state{read_buffer = NewReadBuffer},
  	{noreply,NewState};

handle_info({Closed, Socket}, #state{transport_closed = Closed,socket = Socket} = State) ->
  	{stop, normal, State};

handle_info(_Info, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
set_socket(Client,ListenerPid,Socket,Transport,Opts) ->
	gen_server2:cast(Client, {socket_ready,ListenerPid, Socket,Transport,Opts}).

unpack(Data,Acc) when byte_size(Data) < 4 ->
	{lists:reverse(Acc),Data};
unpack(<<Len:32/integer-signed-big,Payload/bits>> = Data,Acc) when Len > byte_size(Payload) ->
	{lists:reverse(Acc),Data};
unpack(<<Len:32/integer-signed-big, _/bits >> = Data, Acc) ->                                                                                                                                                                                                                                                 
	<< _:32/integer-signed-big,Packet:Len/binary, Rest/bits >> = Data,
  	unpack(Rest,[Packet|Acc]).