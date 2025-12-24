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
import io.modelcontextprotocol.kotlin.sdk.shared.AbstractTransport
import io.modelcontextprotocol.kotlin.sdk.shared.McpJson
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
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
                    val message = runCatching { McpJson.decodeFromString<JSONRPCMessage>(rawMessage) }
                        .onFailure {
                            _onError(it)
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

                    call.respond(HttpStatusCode.Accepted)
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
                            job.cancel()
                        }
                    }
                }
            }
        }.start(wait = false)
    }

    override suspend fun send(message: JSONRPCMessage) {
        val serialized = withContext(Dispatchers.Default) { McpJson.encodeToString(message) }
        if (!outgoing.tryEmit(serialized)) {
            outgoing.emit(serialized)
        }
    }

    override suspend fun close() {
        if (!started.compareAndSet(true, false)) return
        engine?.stop(gracePeriodMillis = 500, timeoutMillis = 5000)
        scope.cancel()
        _onClose()
    }
}
