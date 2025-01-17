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
package io.seata.rm.tcc.interceptor;

import com.alibaba.fastjson.JSON;
import io.seata.common.Constants;
import io.seata.common.exception.FrameworkException;
import io.seata.common.executor.Callback;
import io.seata.common.util.NetUtil;
import io.seata.core.model.BranchType;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.tcc.api.BusinessActionContext;
import io.seata.rm.tcc.api.BusinessActionContextParameter;
import io.seata.rm.tcc.api.TwoPhaseBusinessAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler the TCC Participant Aspect : Setting Context, Creating Branch Record
 * TCC参与者切面处理器
 *
 * @author zhangsen
 */
public class ActionInterceptorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionInterceptorHandler.class);

    /**
     * Handler the TCC Aspect
     *
     * @param method         the method
     * @param arguments      the arguments
     * @param businessAction the business action
     * @param targetCallback the target callback
     * @return map map
     * @throws Throwable the throwable
     */
    public Map<String, Object> proceed(Method method, Object[] arguments, String xid, TwoPhaseBusinessAction businessAction,
                                       Callback<Object> targetCallback) throws Throwable {
        Map<String, Object> ret = new HashMap<String, Object>(16);

        //TCC name
        String actionName = businessAction.name();
        BusinessActionContext actionContext = new BusinessActionContext();
        actionContext.setXid(xid);
        //set action name
        actionContext.setActionName(actionName);
        //TODO services

        //Creating Branch Record
        // 创建分支事务
        String branchId = doTccActionLogStore(method, arguments, businessAction, actionContext);
        actionContext.setBranchId(branchId);

        //set the parameter whose type is BusinessActionContext
        // 记录方法中BusinessActionContext参数的值
        Class<?>[] types = method.getParameterTypes();
        int argIndex = 0;
        for (Class<?> cls : types) {
            if (cls.getName().equals(BusinessActionContext.class.getName())) {
                arguments[argIndex] = actionContext;
                break;
            }
            argIndex++;
        }
        //the final parameters of the try method
        ret.put(Constants.TCC_METHOD_ARGUMENTS, arguments);
        //the final result
        ret.put(Constants.TCC_METHOD_RESULT, targetCallback.execute());
        return ret;
    }

    /**
     * Creating Branch Record
     * 创建分支记录
     *
     * @param method         the method
     * @param arguments      the arguments
     * @param businessAction the business action
     * @param actionContext  the action context
     * @return the string
     */
    protected String doTccActionLogStore(Method method, Object[] arguments, TwoPhaseBusinessAction businessAction,
                                         BusinessActionContext actionContext) {
        String actionName = actionContext.getActionName();
        String xid = actionContext.getXid();
        //
        Map<String, Object> context = fetchActionRequestContext(method, arguments);
        context.put(Constants.ACTION_START_TIME, System.currentTimeMillis());

        //init business context
        // 上下文中加入prepare\commit\rollback等方法名
        initBusinessContext(context, method, businessAction);
        //Init running environment context
        // 初始化运行环境上下文
        initFrameworkContext(context);
        actionContext.setActionContext(context);

        //init applicationData
        // 初始化应用数据
        Map<String, Object> applicationContext = new HashMap<String, Object>(4);
        applicationContext.put(Constants.TCC_ACTION_CONTEXT, context);
        String applicationContextStr = JSON.toJSONString(applicationContext);
        try {
            //registry branch record
            // resourceManager 通过 RPC 向 TC 注册分支事务
            Long branchId = DefaultResourceManager.get().branchRegister(BranchType.TCC, actionName, null, xid,
                applicationContextStr, null);
            // 拿到 TC 返回的分支事务 ID
            return String.valueOf(branchId);
        } catch (Throwable t) {
            String msg = "TCC branch Register error, xid:" + xid;
            LOGGER.error(msg, t);
            throw new FrameworkException(t, msg);
        }
    }

    /**
     * Init running environment context
     *
     * @param context the context
     */
    protected void initFrameworkContext(Map<String, Object> context) {
        try {
            context.put(Constants.HOST_NAME, NetUtil.getLocalIp());
        } catch (Throwable t) {
            LOGGER.warn("getLocalIP error", t);
        }
    }

    /**
     * Init business context
     * 初始化业务上下文
     *
     * @param context        the context
     * @param method         the method
     * @param businessAction the business action
     */
    protected void initBusinessContext(Map<String, Object> context, Method method,
                                       TwoPhaseBusinessAction businessAction) {
        if (method != null) {
            //the phase one method name
            // 第一阶段方法名
            context.put(Constants.PREPARE_METHOD, method.getName());
        }
        if (businessAction != null) {
            //the phase two method name
            context.put(Constants.COMMIT_METHOD, businessAction.commitMethod());
            context.put(Constants.ROLLBACK_METHOD, businessAction.rollbackMethod());
            context.put(Constants.ACTION_NAME, businessAction.name());
        }
    }

    /**
     * Extracting context data from parameters, add them to the context
     * 抽取上下文数据
     *
     * @param method    the method
     * @param arguments the arguments
     * @return map map
     */
    protected Map<String, Object> fetchActionRequestContext(Method method, Object[] arguments) {
        Map<String, Object> context = new HashMap<String, Object>(8);

        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterAnnotations.length; i++) {
            for (int j = 0; j < parameterAnnotations[i].length; j++) {
                if (parameterAnnotations[i][j] instanceof BusinessActionContextParameter) {
                    BusinessActionContextParameter param = (BusinessActionContextParameter)parameterAnnotations[i][j];
                    if (null == arguments[i]) {
                        throw new IllegalArgumentException("@BusinessActionContextParameter 's params can not null");
                    }
                    Object paramObject = arguments[i];
                    int index = param.index();
                    //List, get by index
                    if (index >= 0) {
                        Object targetParam = ((List<Object>)paramObject).get(index);
                        if (param.isParamInProperty()) {
                            context.putAll(ActionContextUtil.fetchContextFromObject(targetParam));
                        } else {
                            context.put(param.paramName(), targetParam);
                        }
                    } else {
                        if (param.isParamInProperty()) {
                            context.putAll(ActionContextUtil.fetchContextFromObject(paramObject));
                        } else {
                            context.put(param.paramName(), paramObject);
                        }
                    }
                }
            }
        }
        return context;
    }

}
