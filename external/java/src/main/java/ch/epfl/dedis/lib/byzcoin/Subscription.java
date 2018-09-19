package ch.epfl.dedis.lib.byzcoin;

import ch.epfl.dedis.lib.exception.CothorityException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Subscription class for ByzCoin. A listener can subscribe to different events and then get notified
 * whenever something happens. This first implementation uses polling to fetch latest blocks and then
 * calls the appropriate receivers. Once we have a streaming service, it will directly connect to the
 * streaming service.
 */
public class Subscription {
    /**
     * A BlockReceiver will be informed on any new block arriving.
     */
    @FunctionalInterface
    public interface BlockReceiver {
        void receive(List<Block> blocks);
    }

    @FunctionalInterface
    public interface ClientTransactionReceiver {
        void receive(List<ClientTransaction> ctxs);
    }

    private ByzCoinRPC bc;
    private Set<BlockReceiver> blockReceivers = new HashSet<>();
    private Set<ClientTransactionReceiver> clientTransactionReceivers = new HashSet<>();
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);
    private Runnable pollRunnable;
    private ScheduledFuture<?> pollHandle;
    private Block latestBlock;

    public Subscription(ByzCoinRPC bc) throws CothorityException {
        this.bc = bc;
        pollRunnable = () -> poll();
    }

    public void subscribeBlock(BlockReceiver br) {
        blockReceivers.add(br);
        updatePolling();
    }

    public void unsubscribeBlock(BlockReceiver br) {
        blockReceivers.remove(br);
        updatePolling();
    }

    public void subscribeClientTransaction(ClientTransactionReceiver ctr) {
        clientTransactionReceivers.add(ctr);
        updatePolling();
    }

    public void unsubscribeClientTransaction(ClientTransactionReceiver ctr) {
        clientTransactionReceivers.remove(ctr);
        updatePolling();
    }

    private void updatePolling() {
        int receivers = blockReceivers.size() +
                clientTransactionReceivers.size();
        if (receivers == 0) {
            stopPolling();
        } else if (receivers == 1) {
            // Might start the polling too often, but startPolling
            // verifies if the polling already started.
            startPolling();
        }
    }

    private void poll() {
        List<ClientTransaction> ctxs = new ArrayList<>();
        List<Block> newBlocks = new ArrayList<>();
        try {
            Block b = bc.getLatestBlock();
            if (!b.equals(latestBlock)) {
                latestBlock = b;
                newBlocks.add(b);
            }
        } catch (CothorityException e) {
            return;
        }
        newBlocks.forEach(b -> ctxs.addAll(b.getClientTransactions()));
        blockReceivers.forEach(br -> br.receive(newBlocks));
        clientTransactionReceivers.forEach(ctr -> ctr.receive(ctxs));
    }

    private void startPolling() {
        if (pollHandle == null) {
            try {
                latestBlock = bc.getLatestBlock();
            } catch (CothorityException e) {
            }
            pollHandle = scheduler.scheduleWithFixedDelay(pollRunnable, 0, bc.getConfig().getBlockInterval().getNano(), NANOSECONDS);
        }
    }

    private void stopPolling() {
        if (pollHandle == null){
            return;
        }
        pollHandle.cancel(false);
        try {
            pollHandle.wait();
            pollHandle = null;
        } catch (InterruptedException e) {
        }
    }
}
