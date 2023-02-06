[![Latest version](https://img.shields.io/maven-central/v/com.xdev-software/xapi-db-informix)](https://mvnrepository.com/artifact/com.xdev-software/xapi-db-informix)
[![Build](https://img.shields.io/github/actions/workflow/status/xdev-software/xapi-db-informix/checkBuild.yml?branch=develop)](https://github.com/xdev-software/xapi-db-informix/actions/workflows/checkBuild.yml?query=branch%3Adevelop)
[![javadoc](https://javadoc.io/badge2/com.xdev-software/xapi-db-informix/javadoc.svg)](https://javadoc.io/doc/com.xdev-software/xapi-db-informix) 
# SqlEngine Database Adapter Informix

The XDEV Application Framework provides an abstraction over database dialects as part of its SqlEngine. This module is the Database Adapter for Informix which includes the Informix-specific implementation for database access.


## Important Note
We once wrote this Database Adapter for Informix 3.50.JC9. Apparently, the oldest version of this driver for maven is 4.10.6 (https://mvnrepository.com/artifact/com.ibm.informix/jdbc). But this is not compatible with our code. 
You therefore, have to integrate your own 3.50.JC9 driver manualy without maven.

## XDEV-IDE
The [XDEV(-IDE)](https://xdev.software/en/products/swing-builder) is a visual Java development environment for fast and easy application development (RAD - Rapid Application Development). XDEV differs from other Java IDEs such as Eclipse or NetBeans, focusing on programming through a far-reaching RAD concept. The IDE's main components are a Swing GUI builder, the XDEV Application Framework and numerous drag-and-drop tools and wizards with which the functions of the framework can be integrated.

The XDEV-IDE was license-free up to version 4 inclusive and is available for Windows, Linux and macOS. From version 5, the previously proprietary licensed additional modules are included in the IDE and the license of the entire product has been converted to a paid subscription model. The XDEV Application Framework, which represents the core of the RAD concept of XDEV and is part of every XDEV application, was released as open-source in 2008.

## Support
If you need support as soon as possible and you can't wait for any pull request, feel free to use [our support](https://xdev.software/en/services/support).

## Contributing
See the [contributing guide](./CONTRIBUTING.md) for detailed instructions on how to get started with our project.

## Dependencies and Licenses
View the [license of the current project](LICENSE) or the [summary including all dependencies](https://xdev-software.github.io/xapi-db-informix/dependencies/)
