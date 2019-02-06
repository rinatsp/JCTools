package org.jctools;

import java.util.Queue;

public class Test {

    public static void main(String[] args) throws InterruptedException {
        Queue<Integer> q = new SpmcLinkedArrayQueue<>(8);

        Thread producer = new Thread(() -> {
            q.offer(1);
            q.offer(3);
            q.offer(4);
            System.out.println("Added");
        });
        producer.start();
        producer.join();
        Thread consumer1 = new Thread(() -> {

            while (q.poll() != null);
            System.out.println("Removed ");
        });
        consumer1.start();
        consumer1.join();
        Thread consumer2 = new Thread(() -> {
            while (q.poll() != null);
            System.out.println("Removed ");
        });
        consumer2.start();
        consumer2.join();

    }
}
