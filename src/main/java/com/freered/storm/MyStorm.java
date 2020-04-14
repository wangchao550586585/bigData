package com.freered.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 模拟storm
 * 
 * @author Administrator
 *
 */
public class MyStorm {
	private Random random = new Random();
	// 接收spout发出的数据--队列1
	private BlockingQueue sentenceQueue = new ArrayBlockingQueue(50000);
	// 接收bolt1发出的数据--队列2
	private BlockingQueue wordQueue = new ArrayBlockingQueue(50000);

	// 保存最后的结果
	private Map<String, Integer> counter = new HashMap<String, Integer>();

	public static void main(String[] args) {
		// 线程池
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		MyStorm myStorm = new MyStorm();
		executorService.submit(new MySpout(myStorm));
		executorService.submit(new MyBoltSplit(myStorm));
		executorService.submit(new MyBoltWordCount(myStorm));
	}

	// 发送句子
	public void nextTuple() {
		String[] sentences = new String[] { "the cow jumped over the moon",
				"an apple a day keeps the doctor away",
				"four score and seven years ago",
				"snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = sentences[random.nextInt(sentences.length)];
		try {
			sentenceQueue.put(sentence);
			System.out.println("send sentence:" + sentence);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 切割句子
	public void split(String sentence) {
		String[] words = sentence.split(" ");
		for (String word : words) {
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				wordQueue.add(word);
				System.out.println("split word:" + word);
			}
		}
	}

	// 计算单词个数
	public void wordCounter(String word) {
		if (counter.containsKey(word)) {
			counter.put(word, counter.get(word) + 1);
		} else {
			counter.put(word, 1);
		}
		System.out.println("print map :" + counter);
	}

	public BlockingQueue getSentenceQueue() {
		return sentenceQueue;
	}

	public void setSentenceQueue(BlockingQueue sentenceQueue) {
		this.sentenceQueue = sentenceQueue;
	}

	public BlockingQueue getWordQueue() {
		return wordQueue;
	}

	public void setWordQueue(BlockingQueue wordQueue) {
		this.wordQueue = wordQueue;
	}

}

class MySpout extends Thread {
	private MyStorm myStorm;

	public MySpout(MyStorm myStorm) {
		this.myStorm = myStorm;
	}

	@Override
	public void run() {
		while (true) {
			myStorm.nextTuple();
			try {
				this.sleep(100);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}

class MyBoltWordCount extends Thread {
	private MyStorm myStorm;

	public MyBoltWordCount(MyStorm myStorm) {
		super();
		this.myStorm = myStorm;
	}

	@Override
	public void run() {
		while (true) {
			try {
				String word = (String) myStorm.getWordQueue().take();
				myStorm.wordCounter(word);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}

class MyBoltSplit extends Thread {
	private MyStorm myStorm;

	public MyBoltSplit(MyStorm myStorm) {
		this.myStorm = myStorm;
	}

	@Override
	public void run() {
		while (true) {
			try {
				String sentence = (String) myStorm.getSentenceQueue().take();
				myStorm.split(sentence);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
