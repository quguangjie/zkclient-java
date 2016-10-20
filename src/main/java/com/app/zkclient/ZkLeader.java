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

import org.omg.stub.java.rmi._Remote_Stub;

import com.github.zkclient.exception.ZkException;

public class ZkLeader extends ZkBase {
	
	private List<ZkNode> _follower;
	private String _LdPath;
	private String _LdNodeName;
	private boolean _LdCreateFlag;

	public ZkLeader(String name, String addr, String serstring) {
		super(name, serstring);
		// TODO Auto-generated constructor stub
		_LdPath = getLeaderPath() + name;
		_LdNodeName = addr;
		_LdCreateFlag = false;
		init();
	}
	
	public ZkLeader(String name, String addr, String serstring, String leaderName) {
		super(name, serstring);
		// TODO Auto-generated constructor stub
		_LdPath = getLeaderPath() + name;
		_LdNodeName = leaderName;
		_LdCreateFlag = false;
		init();
		String fullPath = _LdPath + "/" + _LdNodeName;
		getClient().createPersistent(_LdPath, true);
		getClient().createEphemeral(fullPath);
		_LdCreateFlag = true;
	}
	
	public boolean isLeader() {
		List<String> result;
		try {
			result = getClient().getChildren(_LdPath, false);
			for (String string : result) {
				if (string != null && string.equals(_LdNodeName)) {
					return true;
				}
			}
			return false;
		} catch (ZkException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public List<ZkNode> follower() {
		List<String> result = getClient().getChildren(_LdPath, false);
		if (result != null && result.size() > 0) {
			String string = result.get(0);
			if (string != null && !string.equals(_LdNodeName)) { // ?????
				_follower.clear();
				return _follower;
			}
			result.remove(string);
			if (result.isEmpty()) {
				_follower.clear();
			}
			for (String name : result) {
				ZkNode node = new ZkNode();
				node.setAddr(_LdPath);
				node.setName(name);
				_follower.add(node);
			}
		}
		return _follower;
	}
	
	public void leader(List<ZkNode> follower) {
		
	}

}
