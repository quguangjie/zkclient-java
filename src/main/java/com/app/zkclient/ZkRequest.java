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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.exception.ZkException;
import com.github.zkclient.exception.ZkNoNodeException;
/**
 * 
 * @author eric
 *
 */
public class ZkRequest extends ZkBase implements IZkChildListener {
	
	private String _reqName;
	private String _reqSerFullPath;
	//private WatchReqNodeChild _reqMywc;
	private List<String> _reqSerList;
	private Map<String, String> _reqSerMap;
	
	public Lock reqLock;

	public ZkRequest(String name, String serstring, long ver) {
		super(name, serstring);
		_reqName = name;
		_reqSerFullPath = getSerRegPath() + "/" + name + "/v" + ver;
		System.out.println(_reqSerFullPath);
		
		reqLock = new ReentrantLock();
		_reqSerMap = new IdentityHashMap<String, String>();
		init();
	}

	public boolean discovery() {
		if (!checkNodeExists()) {
			return false;
		}
		//_reqMywc = new WatchReqNodeChild(getClient(), _reqSerFullPath, this);
		try {
			getClient().subscribeChildChanges(_reqSerFullPath, this);
			_reqSerList = getClient().getChildren(_reqSerFullPath);
		} catch (ZkNoNodeException e) {
			e.printStackTrace();
			return false;
		} catch (ZkException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public final List<String> getList() {
		return _reqSerList;
	}
	
	public void setList(List<String> list) {
		_reqSerList = list;
	}
	
	public String getServer(String id) {
		String s = "";
		if (id == null) {
			id = "";
		}
		reqLock.lock();
		changeListToMap();
		if (_reqSerMap == null || _reqSerMap.size() == 0) {
			reqLock.unlock();
			return s;
		}
		Iterator<String> keys = _reqSerMap.keySet().iterator();
		List<String> idList = new ArrayList<String>();
		while (keys.hasNext()) {
			String string = (String) keys.next();
			if (id.equals(string)) {
				idList.add(_reqSerMap.get(string));
			}
		}
		if (idList.size() == 0) {
			reqLock.unlock();
			return s;
		}
		s = idList.get(0);
		reqLock.unlock();
		return s;
	}
	
	private boolean checkNodeExists() {
		try {
			if (getClient() == null) {
				System.err.println("ZkRequest::checkNodeExists getClient is null");
				return false;
			}
			return getClient().exists(_reqSerFullPath);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return false;
		}
	}
	
	private void changeListToMap() {
		_reqSerMap.clear();
		for (String string : _reqSerList) {
			String IDstr="", IPstr="";
			int index = string.indexOf("@");
			if (index > 0) {
				IDstr = string.substring(0, index);
				IPstr = string.substring(index+1, string.length());
			} else {
				IPstr = string;
			}
			_reqSerMap.put(new String(IDstr), IPstr);
		}
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
		// TODO Auto-generated method stub
		setList(currentChildren);
	}
	
	public static void main(String[] args) {
		
		ZkRequest request = new ZkRequest("udbserver", "172.16.1.33:2181,172.16.1.34:2181,172.16.1.35:2181", 1);
		if (request.discovery()) {
			System.out.println("discovery ok");
			String s = request.getServer("");
			if (s != null && !"".equals(s)) {
				System.out.println("find server:" + s);
			} else {
				System.out.println("getServer failed");
			}
		} else {
			System.out.println("discovery failed");
		}
	}

}
