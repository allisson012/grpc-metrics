package com.example.grpc_metrics.controller;

import com.example.grpc_metrics.client.MetricsClient;
import com.example.grpc_metrics.proto.MetricsResponse;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;


@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*") 
public class MetricsController {

    private final MetricsClient metricsClient;
    private long rpcCallCount = 0;

    public MetricsController(MetricsClient metricsClient) {
        this.metricsClient = metricsClient;
    }

    // ---------------------------------------------------------------
    // Endpoint gRPC — retorna a última métrica recebida via stream
    // O stream já está aberto, esse endpoint só lê o que chegou
    // ---------------------------------------------------------------
    @GetMapping("/grpc/metrics")
    public Map<String, Object> getGrpcMetrics() {
        MetricsResponse metrics = metricsClient.getLatestMetrics();

        Map<String, Object> response = new HashMap<>();

        if (metrics == null) {
            response.put("status", "aguardando stream...");
            return response;
        }

        response.put("cpuUsage",        metrics.getCpuUsage());
        response.put("memoryUsage",     metrics.getMemoryUsage());
        response.put("diskUsage",       metrics.getDiskUsage());
        response.put("timestamp",       metrics.getTimestamp());
        response.put("serverId",        metrics.getServerId());
        response.put("streamUpdates",   metricsClient.getStreamUpdateCount());
        response.put("connectionType",  "1 stream aberto (HTTP/2)");

        return response;
    }


    @GetMapping("/rpc/metrics")
    public Map<String, Object> getRpcMetrics() {
        rpcCallCount++;

        long start = System.currentTimeMillis();
        MetricsResponse metrics = metricsClient.getOnce(); 
        long latency = System.currentTimeMillis() - start;

        Map<String, Object> response = new HashMap<>();
        response.put("cpuUsage",        metrics.getCpuUsage());
        response.put("memoryUsage",     metrics.getMemoryUsage());
        response.put("diskUsage",       metrics.getDiskUsage());
        response.put("timestamp",       metrics.getTimestamp());
        response.put("serverId",        metrics.getServerId());
        response.put("rpcCallCount",    rpcCallCount);
        response.put("latencyMs",       latency);
        response.put("connectionType",  "nova conexão por chamada (HTTP/1.1 simulado)");

        return response;
    }

    // ---------------------------------------------------------------
    // Endpoint de status geral — usado pelo dashboard no topo
    // ---------------------------------------------------------------
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("grpcStreamUpdates", metricsClient.getStreamUpdateCount());
        status.put("rpcTotalCalls",     rpcCallCount);
        status.put("grpcPort",          9090);
        status.put("restPort",          8080);
        status.put("uptime",            System.currentTimeMillis());
        return status;
    }
}
