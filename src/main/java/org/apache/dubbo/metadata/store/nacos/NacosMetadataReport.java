/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.metadata.store.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.metadata.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

/**
 * NacosMetadataReport
 */
public class NacosMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(NacosMetadataReport.class);

    private final static String METADATA_GROUP = "dubbo.service.data";


    private final ConfigService configService;


    public NacosMetadataReport(URL url, ConfigService configService) {
        super(url);
        this.configService = configService;
    }


    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String value) {
        storeMetadata(consumerMetadataIdentifier, value);
    }

    private void storeMetadata(MetadataIdentifier metadataIdentifier, String v) {
        try {
            boolean isPublishOk = configService.publishConfig(metadataIdentifier.getIdentifierKey() + META_DATA_SOTRE_TAG, METADATA_GROUP, v);
            if (!isPublishOk) {
                throw new RuntimeException("publish failed");
            }
        } catch (RuntimeException | NacosException e) {
            logger.error("Failed to put " + metadataIdentifier + " to nacos " + v + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + metadataIdentifier + " to nocos " + v + ", cause: " + e.getMessage(), e);
        }
    }


}
