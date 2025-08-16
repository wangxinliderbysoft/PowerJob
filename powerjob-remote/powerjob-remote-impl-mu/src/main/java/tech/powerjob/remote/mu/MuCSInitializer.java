package tech.powerjob.remote.mu;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.remote.framework.actor.ActorInfo;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.cs.CSInitializer;
import tech.powerjob.remote.framework.cs.CSInitializerConfig;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Mu CSInitializer implementation using Netty
 * Supports bidirectional communication with worker-only outbound connectivity
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
public class MuCSInitializer implements CSInitializer {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private CSInitializerConfig config;
    private final ChannelManager channelManager = new ChannelManager();
    
    private MuServerHandler serverHandler;
    private MuWorkerHandler workerHandler;
    private MuConnectionManager connectionManager;

    @Override
    public String type() {
        return "MU";
    }

    @Override
    public void init(CSInitializerConfig config) {
        this.config = config;
        
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        if (config.getServerType() == ServerType.SERVER) {
            initServer();
        } else {
            initWorker();
        }
    }

    private void initServer() {
        try {
            serverHandler = new MuServerHandler(channelManager);
            
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                            .addLast(new IdleStateHandler(0, 0, 60, TimeUnit.SECONDS))
                            .addLast(new MuMessageCodec())
                            .addLast(serverHandler);
                    }
                });

            ChannelFuture future = bootstrap.bind(
                config.getBindAddress().getHost(),
                config.getBindAddress().getPort()
            ).sync();
            
            serverChannel = future.channel();
            log.info("[MuCSInitializer] Server started on {}:{}", 
                config.getBindAddress().getHost(), 
                config.getBindAddress().getPort());
        } catch (Exception e) {
            log.error("[MuCSInitializer] Failed to start server", e);
            throw new RuntimeException("Failed to start Mu server", e);
        }
    }

    private void initWorker() {
        // Worker只初始化handler，不立即连接到server
        // 连接将在第一次发送请求时建立
        workerHandler = new MuWorkerHandler(channelManager);
        connectionManager = new MuConnectionManager(workerGroup, channelManager, workerHandler, config.getBindAddress());
        log.info("[MuCSInitializer] Worker initialized, ready to connect when needed");
    }

    @Override
    public Transporter buildTransporter() {
        return new MuTransporter(channelManager, config.getServerType(), connectionManager);
    }

    @Override
    public void bindHandlers(List<ActorInfo> actorInfos) {
        if (config.getServerType() == ServerType.SERVER && serverHandler != null) {
            serverHandler.bindHandlers(actorInfos);
        } else if (config.getServerType() == ServerType.WORKER && workerHandler != null) {
            workerHandler.bindHandlers(actorInfos);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
            if (connectionManager != null) {
                connectionManager.closeAllConnections();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[MuCSInitializer] Interrupted while closing channels", e);
        } finally {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
        }
        log.info("[MuCSInitializer] Mu CSInitializer closed");
    }
}