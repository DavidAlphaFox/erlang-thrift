-module(thrift_framed_ranch).

-behaviour(thrift_transport).

%% API
-export([new/3]).

%% thrift_transport callbacks
-export([write/2, read/2, flush/1, close/1]).

-record(framed_transport, {
		read_buffer,
        write_buffer,
        module,
		socket
	}).
-type state() :: #framed_transport{}.
-include("thrift_transport_behaviour.hrl").

new(Frame,Module,Socket) ->
    State = #framed_transport{
      read_buffer = Frame,
      write_buffer = undefined,
      module = Module,
      socket = Socket
    },
    thrift_transport:new(?MODULE, State).

%% Writes data into the buffer
write(State = #framed_transport{write_buffer = WBuf}, Data) ->
    {State#framed_transport{write_buffer = [WBuf, Data]}, ok}.

%% Flushes the buffer through to the wrapped transport
flush(State0 = #framed_transport{write_buffer = WBuf,module = M,socket = Socket}) ->
    FrameLen = iolist_size(WBuf),
    Data     = [<<FrameLen:32/integer-signed-big>>, WBuf],
    Response = M:write(Socket,Data),
    State1 = State0#framed_transport{write_buffer = []},
    {State1, Response}.

%% Closes the transport and the wrapped transport
close(State = #framed_transport{module = M,socket = Socket}) ->
    Result = M:close(Socket),
    {State, Result}.

%% Reads data through from the wrapped transport
read(State0 = #framed_transport{ read_buffer = RBuf},Len) when is_integer(Len) ->
    RBufSize = length(RBuf),
    Give = min(RBufSize, Len),
    <<Data:Give/binary, RBuf1/binary>> = iolist_to_binary(RBuf),
    { State0#framed_transport{read_buffer=RBuf1},{ok, Data} }.
