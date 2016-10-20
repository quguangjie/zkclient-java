/**
 * GuangJie Qu <qgjie456@163.com>
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.app.zkclient;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.zkclient.ZkClient;
/**
 * 
 * @author eric
 *
 */
public class ZkBase extends ZkNode {

	private static final int ZKBASE_SESSIONTIMEOUT = 30*1000;
	private static final int ZKBASE_CONNECTIONTIMEOUT = 30*1000;

	private String _serstring;
	private Lock _mutex = new ReentrantLock();// 锁对象
	private static ZkClient _zkc;
	private static String _serRegPath = "/register";
	private static String _serDLockPath = "/lock";
	private static String _serLeaderPath = "/leader";
	private static String _serSessionPath = "/session";

	public ZkBase(String name, String serstring) {
		_serstring = serstring;
	}

	public ZkBase(String name, String addr, String serstring) {
		_serstring = serstring;
	}

	public boolean init() {
		try {
			_mutex.lock();
			if (_zkc == null) {
				_zkc = new ZkClient(_serstring, ZKBASE_SESSIONTIMEOUT, ZKBASE_CONNECTIONTIMEOUT);
				if (_zkc == null) {
					System.out.println("_zkc new Instance ZkClient err");
					return false;
				}
				Thread.sleep(800);
				_zkc.createPersistent(_serRegPath, true);
				_zkc.createPersistent(_serDLockPath, true);
				_zkc.createPersistent(_serLeaderPath, true);
				_zkc.createPersistent(_serSessionPath, true);
			}
			_mutex.unlock();
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			_mutex.unlock();
			e.printStackTrace();
		}
		return false;
	}

	public boolean setServer(String ser) {
		return false;
	}

	public ZkClient getClient() {
		return _zkc;
	}

	public String getSerRegPath() {
		return _serRegPath;
	}

	public String getDLockPath() {
		return _serDLockPath;
	}

	public String getLeaderPath() {
		return _serLeaderPath;
	}

}
