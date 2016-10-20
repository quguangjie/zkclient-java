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

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.zkclient.IZkDataListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;

public class ZkDistributedLock extends ZkBase {
	
	private String _dLockPath;	    //eg. "/lock"
	private String _dLockName;		//eg. " wlock"
	private String _dLockFullPath;	//eg. "/lock/wlock"
	private String _dLockNodeName;	//eg. "/lock/wlock/00001"

	private boolean  DLockStats;
	
	private Lock DLockmutex;
	
	private Condition DLockcond;
	
	private List<String> _dLockList;
	
	private boolean _dLockNoCreate;


	public ZkDistributedLock(String name, String serstring) {
		super(name, serstring);
		init();
		_dLockPath = getDLockPath();
		_dLockList.clear();
		_dLockNoCreate = false;
		_dLockFullPath = "";
		_dLockNodeName = "";
		DLockStats = true;
		
		DLockmutex = new ReentrantLock();
		DLockcond = DLockmutex.newCondition();
	}

	public boolean isNeedCreate() {
		_dLockFullPath = _dLockPath + "/" + _dLockName;
		boolean ret = false;
		try {
			ret = getClient().exists(_dLockFullPath, false);
			if (ret) {
				_dLockNoCreate = true;
				return true;
			}
		} catch (ZkNoNodeException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean Dlock(){
		if (!_dLockNoCreate) {
			if (!isNeedCreate()) {
				getClient().createPersistent(_dLockFullPath, true);
			}
		}
		String lockPath = _dLockFullPath + "/";
		_dLockNodeName = getClient().createEphemeralSequential(lockPath, null);
		while (true) {
			_dLockList = getClient().getChildren(_dLockFullPath, false);
			String minlock = findMinLock();
			if (minlock.equals(_dLockNodeName)) {
				return true;
			}
			WatchDLockNode mywd = new WatchDLockNode(getClient(), this, minlock);
			try {
				getClient().subscribeDataChanges(minlock, mywd);
				DLockmutex.lock();
				while (DLockStats) {
					DLockcond.wait();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			DLockStats = true;
			getClient().unsubscribeDataChanges(minlock, mywd);
			DLockmutex.unlock();
		}
	}
	
	public boolean unDlock() {
		System.out.println("ZkDistributedLock::unDlock: " + _dLockNodeName);
		try {
			getClient().deleteRecursive(_dLockNodeName);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 
	 * @param timeout 秒数
	 * @return
	 */
	public boolean waitDlock(long timeout) {
		if (!_dLockNoCreate) {
			if (!isNeedCreate()) {
				getClient().createPersistent(_dLockFullPath, true);
			}
		}
		String lockPath = _dLockFullPath + "/";
		_dLockNodeName = getClient().createEphemeralSequential(lockPath, null);
		
		_dLockList = getClient().getChildren(_dLockFullPath, false);
		String minlock = findMinLock();
		if (minlock.equals(_dLockNodeName)) {
			return true;
		}
		
		WatchDLockNode mywd = new WatchDLockNode(getClient(), this, minlock);
		try {
			getClient().subscribeDataChanges(minlock, mywd);
			
			DLockmutex.lock();
			while (DLockStats) {
				DLockcond.awaitNanos((System.currentTimeMillis() + timeout*1000L)*1000L);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		DLockStats = true;
		getClient().unsubscribeDataChanges(minlock, mywd);
		DLockmutex.unlock();
		
		_dLockList = getClient().getChildren(_dLockFullPath, false);
		minlock = findMinLock();
		if (minlock.equals(_dLockNodeName)) {
			return true;
		}
		getClient().deleteRecursive(_dLockNodeName);
		return false;
	}
	
	private String findMinLock() {
		String cur;
		String minLock = "";
		String tmp;
		
		int n = _dLockList.size();
		for (int i = 0; i < n; i++) {
			cur = _dLockList.get(i);
			tmp = _dLockFullPath + "/" + cur;
			if (tmp.equals(_dLockNodeName)) {
				if (cur.equals(_dLockList.get(0))) {
					minLock = _dLockNodeName;
					break;
				}
				cur = _dLockList.get(i-1);
				minLock = _dLockFullPath + "/" + cur;
				break;
			}
		}
		return minLock;
	}
	
	public class WatchDLockNode implements IZkDataListener {
		
		ZkDistributedLock pZklock;
		
		public WatchDLockNode(ZkClient cli, ZkDistributedLock plock, String path) {
			// TODO Auto-generated constructor stub
			pZklock = plock;
		}

		@Override
		public void handleDataChange(String dataPath, byte[] data) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
			// TODO Auto-generated method stub
			pZklock.DLockmutex.lock();
			pZklock.DLockStats = false;
			pZklock.DLockcond.signalAll();
			pZklock.DLockmutex.unlock();
		}
		
	}
	
}
