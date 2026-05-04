package com.example.grpc_metrics.client;

import com.example.grpc_metrics.proto.MetricsRequest;
import com.example.grpc_metrics.proto.MetricsResponse;
import com.example.grpc_metrics.proto.MetricsServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

// Marca como componente Spring (pode ser injetado em outros lugares, tipo controller)
@Component
public class MetricsClient {

    // Canal de comunicação com o servidor gRPC
    private ManagedChannel channel;

    // Stub assíncrono → usado para streaming ele permite que o servidor 
    // envie dados continuamente sem bloquear a aplicação.
    private MetricsServiceGrpc.MetricsServiceStub asyncStub;

    private MetricsServiceGrpc.MetricsServiceBlockingStub blockingStub;

    // Guarda a última métrica recebida via streaming
    // "volatile" garante visibilidade correta entre threads
    private volatile MetricsResponse latestMetrics;

    // Contador de quantas atualizações já chegaram via stream
    private volatile int streamUpdateCount = 0;

    // Executado automaticamente quando o Spring sobe a aplicação
    @PostConstruct
    public void init() {

        // Cria a conexão com o servidor gRPC
        channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        // Cria os stubs (interfaces para chamar o servidor)
        asyncStub = MetricsServiceGrpc.newStub(channel);
        blockingStub = MetricsServiceGrpc.newBlockingStub(channel);

        System.out.println("[CLIENT] Conectado ao servidor gRPC em localhost:9090");

        // Já inicia o streaming automaticamente
        startStreaming();
    }

    public MetricsResponse getOnce() {

        System.out.println("[CLIENT] Chamada unária — abrindo nova conexão...");

        MetricsRequest request = MetricsRequest.newBuilder()
                .setIntervalSeconds(1)
                .build();

        MetricsResponse response = blockingStub.getMetricsOnce(request);

        System.out.println("[CLIENT] Resposta recebida — conexão encerrada");

        return response;
    }

    // ---------------------------------------------------------------
    // STREAMING
    // Abre UMA conexão e fica recebendo dados continuamente
    // ---------------------------------------------------------------
    public void startStreaming() {

        System.out.println("[CLIENT] Abrindo stream com servidor...");

        MetricsRequest request = MetricsRequest.newBuilder()
                .setIntervalSeconds(1)
                .build();

        // Inicia o stream (não bloqueia)
        asyncStub.watchMetrics(request, new StreamObserver<MetricsResponse>() {

            // Chamado toda vez que o servidor envia um novo dado
            @Override
            public void onNext(MetricsResponse metrics) {

                latestMetrics = metrics; // salva a última métrica
                streamUpdateCount++;     

                System.out.println("[CLIENT] Métrica #" + streamUpdateCount
                        + " recebida via stream — CPU: "
                        + String.format("%.1f", metrics.getCpuUsage()) + "%");
            }

            // Chamado se der erro no stream
            @Override
            public void onError(Throwable t) {

                System.err.println("[CLIENT] Erro no stream: " + t.getMessage());

                // Estratégia simples: tenta reconectar depois de 3 segundos
                try {
                    Thread.sleep(3000);
                    startStreaming();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Chamado quando o servidor encerra o stream
            @Override
            public void onCompleted() {
                System.out.println("[CLIENT] Stream encerrado pelo servidor");
            }
        });
    }

    // Método usado por outras partes da aplicação (ex: controller REST)
    public MetricsResponse getLatestMetrics() {
        return latestMetrics;
    }

    // Retorna quantas atualizações já chegaram
    public int getStreamUpdateCount() {
        return streamUpdateCount;
    }

    // Executado quando a aplicação vai encerrar
    @PreDestroy
    public void shutdown() throws InterruptedException {

        System.out.println("[CLIENT] Encerrando conexão gRPC...");

        // Fecha o canal com o servidor
        channel.shutdown();
    }
}