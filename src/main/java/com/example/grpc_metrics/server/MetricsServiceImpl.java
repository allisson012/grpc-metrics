package com.example.grpc_metrics.server;

import com.example.grpc_metrics.proto.MetricsRequest;
import com.example.grpc_metrics.proto.MetricsResponse;
import com.example.grpc_metrics.proto.MetricsServiceGrpc;

import io.grpc.stub.StreamObserver;

import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@GrpcService  
public class MetricsServiceImpl extends MetricsServiceGrpc.MetricsServiceImplBase {

    // Gerador de números aleatórios (usado pra simular métricas)
    private final Random random = new Random();

    private double cpuBase = 45.0;
    private double memBase = 60.0;

    @Override
    public void getMetricsOnce(MetricsRequest request,
                               StreamObserver<MetricsResponse> responseObserver) {

        System.out.println("[UNÁRIO] Requisição recebida — respondendo uma vez e fechando");

        MetricsResponse response = buildMetrics();

        responseObserver.onNext(response);

        responseObserver.onCompleted();
    }

    // ---------------------------------------------------------------
    // RPC STREAMING
    // Cliente pede → servidor envia várias respostas ao longo do tempo
    // ---------------------------------------------------------------
    @Override
    public void watchMetrics(MetricsRequest request,
                             StreamObserver<MetricsResponse> responseObserver) {

        // Pega o intervalo enviado pelo cliente (ou usa 1s como padrão)
        int intervalSeconds = request.getIntervalSeconds() > 0
                ? request.getIntervalSeconds() : 1;

        System.out.println("[STREAM] Stream aberto — enviando métricas a cada "
                + intervalSeconds + "s indefinidamente");

        // Cria uma thread separada 
        Thread streamThread = new Thread(() -> {
            try {
                int count = 0;

                // Loop infinito até o cliente encerrar
                while (!Thread.currentThread().isInterrupted()) {

                    // Gera uma nova métrica
                    MetricsResponse metrics = buildMetrics();

                    // Envia pro cliente sem ele precisar pedir novamente
                    responseObserver.onNext(metrics);

                    count++;
                    System.out.println("[STREAM] Enviando métrica #" + count
                            + " — CPU: " + String.format("%.1f", metrics.getCpuUsage()) + "%");

                    // Espera o intervalo definido
                    TimeUnit.SECONDS.sleep(intervalSeconds);
                }

            } catch (InterruptedException e) {
                System.out.println("[STREAM] Stream encerrado pelo cliente");
                Thread.currentThread().interrupt();

            } catch (Exception e) {
                responseObserver.onError(e);
                return;
            }

            // Finaliza o stream corretamente
            responseObserver.onCompleted();
        });

        // Define como thread "daemon" (morre junto com a aplicação)
        streamThread.setDaemon(true);

        // Inicia a thread
        streamThread.start();
    }

    // ---------------------------------------------------------------
    // Método que gera métricas falsas
    // ---------------------------------------------------------------
    private MetricsResponse buildMetrics() {

        
        boolean spike = random.nextDouble() < 0.08;

        cpuBase = spike
                ? 85 + random.nextDouble() * 14
                : Math.min(99, Math.max(5, cpuBase + (random.nextDouble() * 20 - 10)));

        memBase = Math.min(99, Math.max(20, memBase + (random.nextDouble() * 10 - 5)));

        double disk = 30 + random.nextDouble() * 25;

        if (spike) {
            System.out.println("[STREAM] *** PICO DE CPU DETECTADO: "
                    + String.format("%.1f", cpuBase) + "% ***");
        }

        // Monta o objeto de resposta (padrão Builder do protobuf)
        return MetricsResponse.newBuilder()
                .setCpuUsage(Math.round(cpuBase * 10.0) / 10.0)
                .setMemoryUsage(Math.round(memBase * 10.0) / 10.0)
                .setDiskUsage(Math.round(disk * 10.0) / 10.0)
                .setTimestamp(System.currentTimeMillis())
                .setServerId("server-01")
                .build();
    }
}