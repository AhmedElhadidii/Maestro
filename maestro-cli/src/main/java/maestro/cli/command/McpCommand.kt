package maestro.cli.command

import picocli.CommandLine
import java.util.concurrent.Callable
import maestro.cli.mcp.runMaestroMcpServer
import java.io.File
import maestro.cli.util.WorkingDirectory

@CommandLine.Command(
    name = "mcp",
    description = [
        "Starts the Maestro MCP server, exposing Maestro device and automation commands as Model Context Protocol (MCP) tools over STDIO or HTTP/SSE for LLM agents and automation clients."
    ],
)
class McpCommand : Callable<Int> {
    @CommandLine.Option(
        names = ["--working-dir"],
        description = ["Base working directory for resolving files"]
    )
    private var workingDir: File? = null

    @CommandLine.Option(
        names = ["--http"],
        description = ["Expose the MCP server over HTTP + SSE instead of STDIO"]
    )
    private var httpTransport: Boolean = false

    @CommandLine.Option(
        names = ["--http-host"],
        description = ["Host/interface to bind when running with --http (default: \${DEFAULT-VALUE})"],
        defaultValue = "0.0.0.0"
    )
    private var httpHost: String = "0.0.0.0"

    @CommandLine.Option(
        names = ["--http-port"],
        description = ["Port to bind when running with --http (default: \${DEFAULT-VALUE})"],
        defaultValue = "7090"
    )
    private var httpPort: Int = 7090

    override fun call(): Int {
        if (workingDir != null) {
            WorkingDirectory.baseDir = workingDir!!.absoluteFile
        }
        runMaestroMcpServer(
            useHttpTransport = httpTransport,
            httpHost = httpHost,
            httpPort = httpPort
        )
        return 0
    }
} 
