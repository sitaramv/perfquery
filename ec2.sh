#!/bin/bash

hosts=(ec2-44-234-51-210.us-west-2.compute.amazonaws.com ec2-34-222-183-193.us-west-2.compute.amazonaws.com  ec2-44-234-150-1.us-west-2.compute.amazonaws.com ec2-35-82-12-3.us-west-2.compute.amazonaws.com ec2-44-235-91-186.us-west-2.compute.amazonaws.com ec2-35-88-7-28.us-west-2.compute.amazonaws.com)
servicenames=(data data data index query test)
rpmfile="couchbase-server-enterprise.rpm"
kfile="sitaram-oregon.pem"
aws=false
shell=false
install=false
cluster=false
lvm=false
ramsize=25000

POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -a|--aws)
      aws=true
      shift 
      ;;
    -l|--lvm)
      lvm=true
      shift 
      ;;
    -s|--shell)
      shell=true
      shift
      ;;
    -i|--install)
      install=true
      shift
      ;;
    -c|--cluster)
      cluster=true
      shift
      ;;
    -k)
      shift
      kfile=$1
      shift
      ;;
    -r|--rpm)
      shift
      rpmfile=$1
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters

doaws() {
    i=0 
    for h in "${hosts[@]}"
    do 
	service=${servicenames[${i}]}
        scp -i $kfile -o StrictHostKeychecking=no $rpmfile ec2.sh ec2-user@$h:/home/ec2-user
        if [[ "$service" == "data" || "$service" == "index" ]]; then
            ssh -i $kfile -o StrictHostKeychecking=no ec2-user@$h sudo ./ec2.sh -l
	fi
        ssh -i $kfile -o StrictHostKeychecking=no ec2-user@$h sudo ./ec2.sh -i -r $rpmfile
        if [[ "$service" == "test" ]]; then
             ssh -i $kfile -o StrictHostKeychecking=no ec2-user@$h sudo service couchbase-server stop
	fi
        ((i=i+1))
    done
    ssh -i $kfile -o StrictHostKeychecking=no ec2-user@${hosts[0]} sudo ./ec2.sh -c
}


doshell() {
    ssh -i $kfile -o StrictHostKeychecking=no ec2-user@"${hosts[$1]}"
}

docluster() {
    i=0 
    for h in "${hosts[@]}"
    do
	service=${servicenames[${i}]}
        if [[ $i -eq  0 ]]; then
            /opt/couchbase/bin/couchbase-cli cluster-init -c localhost --cluster-username Administrator --cluster-password password \
	        --services $service  --cluster-ramsize $ramsize --cluster-index-ramsize $ramsize --index-storage-setting default
        elif [[ "$service" != "test" ]]; then
            /opt/couchbase/bin/couchbase-cli server-add -c localhost --username Administrator \
                --password password --server-add https://$h:18091 \
                --server-add-username Administrator --server-add-password password \
                --services $service
        fi
	/opt/couchbase/bin/couchbase-cli rebalance -c localhost -u Administrator -p password
        ((i=i+1))
    done
}

dolvm() {
     if [[ ! -d /data ]]; then
         # MB-52071
         lsblk
         disksize=`lsblk | grep nvme2n1 | awk '{print $4}'`
	 if [[ "$disksize" == "884.8G" ]] ; then
	     instance=nvme2n1
	     ebs=nvme1n1
         else
	     instance=nvme1n1
	     ebs=nvme2n1
         fi
         # Create PV from instance store
         pvcreate /dev/$instance
         # Create PV from EBS volums
         pvcreate /dev/$ebs
         # Create volume group ‘VG_CB’ containing both
         vgcreate VG_CB /dev/$instance /dev/$ebs
         # Create LV from origin EBS volume
         lvcreate --extents 100%PVS -n LV_data_ebs VG_CB /dev/$ebs
         # Create cache-pool LV from instance storage
         lvcreate --type cache-pool --extents 100%PVS -n LV_data_cache_pool VG_CB /dev/$instance
         # Create a cached logical volume by associating the cache pool with EBS
         lvconvert --yes --type cache --cachepool LV_data_cache_pool VG_CB/LV_data_ebs
         # Create filesystem on cached volume:
         mkfs.xfs /dev/VG_CB/LV_data_ebs
         umask 0
         mkdir -p /data
         mount /dev/VG_CB/LV_data_ebs /data
         chown -R couchbase:couchbase /data
     fi
     lsblk
     lvs -a
}

doinstall() {
     yum -y install ncurses-compat-libs
     yum -y install git
     yum -y install go
     rpm --install $rpmfile
     (umask 0; mkdir -p /data/backups /data/data; chown -R couchbase:couchbase /data; chmod 777 /data/backups)
     sudo -u ec2-user git clone https://github.com/sitaramv/perfquery.git
     (cd perfquery; go build -o load_data main.go)
     sleep 10
     /opt/couchbase/bin/couchbase-cli node-init -c localhost --node-init-data-path /data/data --node-init-index-path /data/data -u Administrator -p password
     #git push -u origin main
}

if [ "$aws" == true ] ; then
    if [[ ! -f "${rpmfile}" && ! -h "${rpmfile}" ]] ; then
         echo "$rpmfile not present "
	 exit 1
    fi

    if [[ ! -f "${kfile}" && ! -h "${kfile}" ]] ; then
         echo "$kfile not present "
	 exit 1
    fi
    doaws
fi

if [ "$shell" == true ] ; then
    if [[ ! -f "${kfile}" && ! -h "${kfile}" ]] ; then
         echo "$kfile not present "
	 exit 1
    fi
    doshell
fi


if [ "$lvm" == true ] ; then
    dolvm
fi

if [ "$install" == true ] ; then
    if [[ ! -f "${rpmfile}" && ! -h "${rpmfile}" ]] ; then
         echo "$rpmfile not present "
	 exit 1
    fi
    doinstall
fi

if [ "$cluster" == true ] ; then
    docluster
fi

exit 0
