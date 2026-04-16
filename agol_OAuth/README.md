# ArcGIS OAuth Embedded Map (Minimal Pattern Example)

###### 

###### This project demonstrates a minimal implementation of \*\*OAuth 2.0 authentication with ArcGIS Online\*\* using the ArcGIS Maps SDK for JavaScript and a custom HTML application.

###### 

###### It is intended as a \*\*reference implementation pattern\*\*, showing how to embed a secured ArcGIS web map outside of Esri out-of-the-box applications (such as Experience Builder or Instant Apps).



# 🔍 What This Toolkit Does

###### 

###### OAuth 2.0 authentication with ArcGIS Online

###### Use of `IdentityManager` for credential handling

###### Use of `OAuthInfo` for app registration

###### Embedding an ArcGIS web map using `<arcgis-map>` web component

###### Minimal custom UI wrapping a secured GIS application



# 🧱 Architecture Overview



###### This is a single-page HTML application that combines:

###### 

###### \*\*ArcGIS Maps SDK for JavaScript (via CDN)\*\*

###### \*\*OAuth authentication flow (popup disabled, redirect-based)\*\*

###### \*\*Declarative web map embedding\*\*

###### Minimal UI layer for presentation

###### 

###### The goal is clarity and simplicity, not feature completeness.



# 🔐 Authentication Flow



###### Authentication is handled using:

###### 

###### `OAuthInfo` → defines ArcGIS app registration

###### `IdentityManager` → manages login state and credentials

###### 

###### Flow:

###### 

###### 1\. Attempt silent sign-in via `checkSignInStatus()`

###### 2\. If not authenticated, trigger login via `getCredential()`

###### 3\. Store session via ArcGIS IdentityManager



# 👤 Author



Andrew Sheppard

GIS Developer | Solutions Engineer | Automation Specialist

