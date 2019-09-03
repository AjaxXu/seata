/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.tcc.remoting.parser;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.seata.common.exception.FrameworkException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.ReflectionUtil;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.tcc.TCCResource;
import io.seata.rm.tcc.api.BusinessActionContext;
import io.seata.rm.tcc.api.TwoPhaseBusinessAction;
import io.seata.rm.tcc.remoting.RemotingDesc;
import io.seata.rm.tcc.remoting.RemotingParser;

/**
 * parsing remoting bean
 * 解析远程bean
 *
 * @author zhangsen
 */
public class DefaultRemotingParser {

    /**
     * all remoting bean parser
     */
    protected static List<RemotingParser> allRemotingParsers = new ArrayList<>();

    /**
     * all remoting beans beanName -> RemotingDesc
     */
    protected static Map<String, RemotingDesc> remotingServiceMap = new ConcurrentHashMap<String, RemotingDesc>();

    private static class SingletonHolder {
        private static DefaultRemotingParser INSTANCE = new DefaultRemotingParser();
    }

    /**
     * Get resource manager.
     *
     * @return the resource manager
     */
    public static DefaultRemotingParser get() {
        return DefaultRemotingParser.SingletonHolder.INSTANCE;
    }

    /**
     * Instantiates a new Default remoting parser.
     * 实例化一个新的默认的远程解析器
     */
    protected DefaultRemotingParser() {
        initRemotingParser();
    }

    /**
     * init parsers
     * 获得所有RemotingParser的实现类实例对象
     */
    protected void initRemotingParser() {
        //init all resource managers
        List<RemotingParser> remotingParsers = EnhancedServiceLoader.loadAll(RemotingParser.class);
        if (CollectionUtils.isNotEmpty(remotingParsers)) {
            allRemotingParsers.addAll(remotingParsers);
        }
    }

    /**
     * is remoting bean ?
     * 由具体的RemotingParser实现类判断
     *
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     */
    public boolean isRemoting(Object bean, String beanName) {
        for (RemotingParser remotingParser : allRemotingParsers) {
            if (remotingParser.isRemoting(bean, beanName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is reference bean?
     *
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     */
    public boolean isReference(Object bean, String beanName) {
        for (RemotingParser remotingParser : allRemotingParsers) {
            if (remotingParser.isReference(bean, beanName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is service bean ?
     *
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     */
    public boolean isService(Object bean, String beanName) {
        for (RemotingParser remotingParser : allRemotingParsers) {
            if (remotingParser.isService(bean, beanName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * get the remoting Service desc
     * 获得远程服务的描述信息
     *
     * @param bean     the bean
     * @param beanName the bean name
     * @return service desc
     */
    public RemotingDesc getServiceDesc(Object bean, String beanName) {
        List<RemotingDesc> ret = new ArrayList<RemotingDesc>();
        for (RemotingParser remotingParser : allRemotingParsers) {
            RemotingDesc s = remotingParser.getServiceDesc(bean, beanName);
            if (s != null) {
                ret.add(s);
            }
        }
        if (ret.size() == 1) {
            return ret.get(0);
        } else if (ret.size() > 1) {
            throw new FrameworkException("More than one RemotingParser for bean:" + beanName);
        } else {
            return null;
        }
    }

    /**
     * parse the remoting bean info
     * 解析远程bean的信息
     *
     * @param bean     the bean
     * @param beanName the bean name
     * @return remoting desc
     */
    public RemotingDesc parserRemotingServiceInfo(Object bean, String beanName) {
        RemotingDesc remotingBeanDesc = getServiceDesc(bean, beanName);
        if (remotingBeanDesc == null) {
            return null;
        }
        remotingServiceMap.put(beanName, remotingBeanDesc);

        Class<?> interfaceClass = remotingBeanDesc.getInterfaceClass();
        Method[] methods = interfaceClass.getMethods();
        if (isService(bean, beanName)) {
            try {
                //service bean， registry resource
                // 如果是 TCC service Bean，解析并注册该 resource
                Object targetBean = remotingBeanDesc.getTargetBean();
                for (Method m : methods) {
                    TwoPhaseBusinessAction twoPhaseBusinessAction = m.getAnnotation(TwoPhaseBusinessAction.class);
                    if (twoPhaseBusinessAction != null) {
                        // 如果有 TCC 参与方注解，定义一个 TCCResource
                        TCCResource tccResource = new TCCResource();
                        tccResource.setActionName(twoPhaseBusinessAction.name());
                        // TCC Bean
                        tccResource.setTargetBean(targetBean);
                        // try 方法
                        tccResource.setPrepareMethod(m);
                        tccResource.setCommitMethodName(twoPhaseBusinessAction.commitMethod());
                        // confirm 方法对象
                        tccResource.setCommitMethod(ReflectionUtil
                            .getMethod(interfaceClass, twoPhaseBusinessAction.commitMethod(),
                                new Class[] {BusinessActionContext.class}));
                        tccResource.setRollbackMethodName(twoPhaseBusinessAction.rollbackMethod());
                        // cancel 方法对象
                        tccResource.setRollbackMethod(ReflectionUtil
                            .getMethod(interfaceClass, twoPhaseBusinessAction.rollbackMethod(),
                                new Class[] {BusinessActionContext.class}));
                        // registry tcc resource
                        // 调用到 TCCResourceManager 的 registerResource 方法
                        DefaultResourceManager.get().registerResource(tccResource);
                    }
                }
            } catch (Throwable t) {
                throw new FrameworkException(t, "parser remting service error");
            }
        }
        if (isReference(bean, beanName)) {
            //reference bean， TCC proxy
            remotingBeanDesc.setReference(true);
        }
        return remotingBeanDesc;
    }

    /**
     * Get remoting bean desc remoting desc.
     *
     * @param beanName the bean name
     * @return the remoting desc
     */
    public RemotingDesc getRemotingBeanDesc(String beanName) {
        return remotingServiceMap.get(beanName);
    }

}
