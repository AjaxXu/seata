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
package io.seata.rm.datasource.exec;

import java.sql.SQLException;

/**
 * The type Lock conflict exception.
 * 锁冲突异常
 *
 * @author sharajava
 */
public class LockConflictException extends SQLException {

    /**
     * Instantiates a new Lock conflict exception.
     */
    public LockConflictException() {
    }

    public LockConflictException(String message) {
        super(message);
    }
}
