#!/bin/bash

getTestData(){
	count=$(($1/4))
	pth=$(basename `pwd`)
	mkdir /home/xhahn/project/naivebayes/data/testdata/$pth
	for file in ./*
		do
		if((count==0));then
			break
		else
			mv $file /home/xhahn/project/naivebayes/data/testdata/$pth
			let --count
		fi
		done
	}

pre(){
	count=0
	for file in $1/*
	do
		if test -d $file;then
			cd $file
			pre $file
		else
			let ++count
		fi
	done

	if((count<100));then
		rm -r $(pwd)
	else
		getTestData $count
	fi	
}

pre /home/xhahn/project/naivebayes/data/train/NBCorpus
