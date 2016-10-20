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

import com.github.zkclient.IZkDataListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkException;

public class ZkAccept extends ZkLeader implements IZkDataListener {

	private String _acPath;
	private String _acNodeName;
	
	public ZkAccept(String name, String addr, String serstring, long ver) {
		// TODO Auto-generated constructor stub
		super(name, addr, serstring);
		_acPath = getSerRegPath() + "/" + name + "/v" + ver;
		System.out.println("_acPath : " + _acPath);
		_acNodeName = addr;
	}
	
	public ZkAccept(String name, String addr, String serstring, String leaderName, long ver) {
		// TODO Auto-generated constructor stub
		super(name, addr, serstring, leaderName);
		_acPath = getSerRegPath() + "/" + name + "/v" + ver;
		_acNodeName = addr;
	}
	
	public boolean serRegister() {
		getClient().createPersistent(_acPath, true);
		String fullPath = _acPath + "/" + _acNodeName;
		while(true)
		{
			try{
				getClient().deleteRecursive(fullPath); 
				getClient().createEphemeral(fullPath);
				getClient().subscribeDataChanges(fullPath, this);
				break;
			} catch (ZkException e){
				e.printStackTrace();
			}
		}
		return true;
	}
	
	public void leader(List<ZkNode> follow) {
		
	}

	@Override
	public void handleDataChange(String dataPath, byte[] data) throws Exception {
		
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		try {
			getClient().createEphemeral(dataPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
