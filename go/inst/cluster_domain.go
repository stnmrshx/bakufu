/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

func ApplyClusterDomain(clusterInfo *ClusterInfo) {
	clusterInfo.ClusterDomain, _ = ReadClusterDomainName(clusterInfo.ClusterName)
}
