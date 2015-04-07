#!/bin/sh

BUILD_NUMBER=25

ssh ddeploy01.tag-dev.com <<!
tds jenkinspackage add satisfaction-scheduler jbanks-satisfaction-limbo $BUILD_NUMBER
!
