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
package io.seata.rm.datasource.undo;

/**
 * The interface Keyword checker.
 * 关键字检查
 *
 * @author Wu
 * @date 2019 /3/5 The interface Keyword checker
 */
public interface KeywordChecker {
    /**
     * check whether given field name and table name use keywords
     *
     * @param fieldOrTableName the field or table name
     * @return boolean
     */
    boolean check(String fieldOrTableName);

    /**
     * check whether given field name and table name use keywords and,if so,will add "`" to the name.
     *
     * @param fieldOrTableName the field or table name
     * @return string
     */
    String checkAndReplace(String fieldOrTableName);
}
