package co.vinni.dmos.server;

import co.vinni.dmos.config.WebSocketConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(WebSocketConfig.class)
public class NetworkApplication {

    @Value("${server.port:8860}")
    private String port;

    public static void main(String[] args) {
        SpringApplication.run(NetworkApplication.class, args);
    }

    @PostConstruct
    public void init() {
        System.out.println("\n==========================================================");
        System.out.println("Servidor WebSocket Multisala activo en:");
        System.out.println("  Web:       http://localhost:" + port + "/");
        System.out.println("  WebSocket: ws://localhost:" + port + "/network-monitor");
        System.out.println("==========================================================\n");
    }
}