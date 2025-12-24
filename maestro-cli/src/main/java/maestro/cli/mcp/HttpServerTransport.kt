package maestro.cli.mcp

import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.engine.ApplicationEngine
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondTextWriter
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.modelcontextprotocol.kotlin.sdk.JSONRPCMessage
import io.modelcontextprotocol.kotlin.sdk.JSONRPCMessagePolymorphicSerializer
import io.modelcontextprotocol.kotlin.sdk.shared.AbstractTransport
import io.modelcontextprotocol.kotlin.sdk.shared.McpJson
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean

class HttpServerTransport(
    private val host: String = "0.0.0.0",
    private val port: Int = 7090,
) : AbstractTransport() {

    companion object {
        private const val DEFAULT_GRACE_PERIOD_MS = 500L
        private const val DEFAULT_STOP_TIMEOUT_MS = 5000L
    }

    private val started = AtomicBoolean(false)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val outgoing =
        MutableSharedFlow<String>(replay = 0, extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    private var engine: ApplicationEngine? = null

    override suspend fun start() {
        if (!started.compareAndSet(false, true)) error("HTTP transport already started")

        engine = embeddedServer(Netty, port = port, host = host) {
            routing {
                post("/rpc") {
                    val rawMessage = call.receiveText()
                    val message = runCatching { McpJson.decodeFromString(JSONRPCMessagePolymorphicSerializer, rawMessage) }
                        .onFailure { cause ->
                            _onError(IllegalArgumentException("Invalid MCP JSON-RPC payload", cause))
                        }
                        .getOrNull()

                    if (message == null) {
                        call.respond(HttpStatusCode.BadRequest, "Invalid MCP JSON-RPC payload")
                        return@post
                    }

                    scope.launch {
                        try {
                            _onMessage(message)
                        } catch (e: Throwable) {
                            _onError(e)
                        }
                    }

                    call.respond(
                        HttpStatusCode.Accepted,
                        "Message accepted; responses and notifications stream via /events"
                    )
                }

                get("/events") {
                    call.response.headers.append(HttpHeaders.CacheControl, "no-cache")
                    call.respondTextWriter(contentType = ContentType.Text.EventStream) {
                        val job = scope.launch {
                            outgoing.collect { json ->
                                write("data: $json\n\n")
                                flush()
                            }
                        }
                        try {
                            awaitCancellation()
                        } finally {
                            job.cancelAndJoin()
                        }
                    }
                }
            }
        }.start(wait = false)
    }

    override suspend fun send(message: JSONRPCMessage) {
        val serialized = withContext(Dispatchers.Default) { McpJson.encodeToString(JSONRPCMessagePolymorphicSerializer, message) }
        if (!outgoing.tryEmit(serialized)) {
            val error = IllegalStateException("Unable to deliver MCP message over SSE (buffer full or no active subscribers)")
            System.err.println("MCP HTTP transport: ${error.message}")
            _onError(error)
        }
    }

    override suspend fun close() {
        if (!started.compareAndSet(true, false)) return
        engine?.stop(gracePeriodMillis = DEFAULT_GRACE_PERIOD_MS, timeoutMillis = DEFAULT_STOP_TIMEOUT_MS)
        scope.cancel()
        _onClose()
    }
}
