//
//  Server.swift
//  feather
//
//  Created by samara on 22.08.2024.
//  Copyright © 2024 Lakr Aream. All Rights Reserved.
//  ORIGINALLY LICENSED UNDER GPL-3.0, MODIFIED FOR USE FOR FEATHER
//

import Foundation
import Vapor
import NIOSSL
import NIOTLS
import NIOCore
import NIOPosix
import SwiftUI
import IDeviceSwift

// MARK: - Class
class ServerInstaller: Identifiable, ObservableObject {
	let id = UUID()
	let port = Int.random(in: 4000...8000)
	private var _needsShutdown = false
	
	var packageUrl: URL?
	var app: AppInfoPresentable
	@ObservedObject var viewModel: InstallerStatusViewModel
	private var _server: Application?
	@Published private(set) var payloadBytesSent: Int64 = 0
	@Published private(set) var payloadTotalBytes: Int64 = 0
	@Published private(set) var payloadBytesPerSecond: Double = 0
	@Published private(set) var payloadLastActivity: Date?
	@Published private(set) var payloadPhase: String = "IN ATTESA"
	@Published private(set) var payloadBytesRead: Int64 = 0
	@Published private(set) var payloadRequestCount: Int = 0
	@Published private(set) var payloadRequestInfo: String = ""

	init(app: AppInfoPresentable, viewModel: InstallerStatusViewModel) throws {
		self.app = app
		self.viewModel = viewModel
		try _setup()
		try _configureRoutes()
		try _server?.server.start()
		_needsShutdown = true
	}
	
	deinit {
		_shutdownServer()
	}
	
	private func _setup() throws {
		self._server = try? setupApp(port: port)
	}
		
	private func _configureRoutes() throws {
		_server?.get("*") { [weak self] req in
			guard let self else { return Response(status: .badGateway) }
			switch req.url.path {
			case plistEndpoint.path:
				self._updateStatus(.sendingManifest)
				return Response(status: .ok, version: req.version, headers: [
					"Content-Type": "text/xml",
				], body: .init(data: installManifestData))
			case displayImageSmallEndpoint.path:
				return Response(status: .ok, version: req.version, headers: [
					"Content-Type": "image/png",
				], body: .init(data: displayImageSmallData))
			case displayImageLargeEndpoint.path:
				return Response(status: .ok, version: req.version, headers: [
					"Content-Type": "image/png",
				], body: .init(data: displayImageLargeData))
			case payloadEndpoint.path:
				guard let packageUrl = packageUrl else {
					return Response(status: .notFound)
				}
				
				self._notePayloadRequest(req)
				self._updateStatus(.sendingPayload)
				return self._streamPayload(at: packageUrl, for: req)
			case "/install":
				var headers = HTTPHeaders()
				headers.add(name: .contentType, value: "text/html")
				return Response(status: .ok, headers: headers, body: .init(string: self.html))
			default:
				return Response(status: .notFound)
			}
		}
	}
	
	private func _shutdownServer() {
		guard _needsShutdown else { return }
		
		_needsShutdown = false
		_server?.server.shutdown()
		_server?.shutdown()
	}
	
	private func _updateStatus(_ newStatus: InstallerStatusViewModel.InstallerStatus) {
		DispatchQueue.main.async {
			self.viewModel.status = newStatus
		}
	}

	private func _setPayloadPhase(_ phase: String) {
		DispatchQueue.main.async {
			self.payloadPhase = phase
			self.payloadLastActivity = Date()
		}
	}

	private func _notePayloadRequest(_ req: Request) {
		let method = String(describing: req.method)
		let range = req.headers.first(name: "Range") ?? "none"
		DispatchQueue.main.async {
			self.payloadRequestCount += 1
			self.payloadRequestInfo = "\(method), Range: \(range)"
			self.payloadPhase = "RICHIESTA HTTP RICEVUTA"
			self.payloadBytesRead = 0
			self.payloadBytesSent = 0
			self.payloadBytesPerSecond = 0
			self.payloadLastActivity = Date()
		}
	}

	/// Preserve Vapor's HTTP headers and byte-range handling while counting the
	/// bytes accepted by the response writer. These are NOT network ACKs from iOS.
	private func _streamPayload(at url: URL, for req: Request) -> Response {
		let response = req.fileio.streamFile(at: url.path)
		_setPayloadPhase("RISPOSTA HTTP PREPARATA")
		guard response.status == .ok || response.status == .partialContent else {
			return response
		}

		let fileSize = (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.int64Value ?? 0
		let responseBytes = response.body.count
		guard fileSize > 0, responseBytes > 0 else {
			return req.fileio.streamFile(at: url.path) { [weak self] result in
				self?._finishPayload(result)
			}
		}

		var offset: Int64 = 0
		if response.status == .partialContent {
			guard let range = response.headers.contentRange,
				case let .withinWithLimit(start, _, _) = range.range else {
				return req.fileio.streamFile(at: url.path) { [weak self] result in
					self?._finishPayload(result)
				}
			}
			offset = Int64(start)
		}

		let tracker = PayloadProgressTracker(startOffset: offset, totalBytes: fileSize) { [weak self] sent, speed in
			DispatchQueue.main.async {
				self?.payloadBytesSent = sent
				self?.payloadTotalBytes = fileSize
				self?.payloadBytesPerSecond = speed
				self?.payloadLastActivity = Date()
			}
		}

		DispatchQueue.main.async {
			self.payloadBytesSent = offset
			self.payloadTotalBytes = fileSize
			self.payloadBytesPerSecond = 0
			self.payloadLastActivity = Date()
		}

		response.body = .init(stream: { [weak self] writer in
			self?._setPayloadPhase("WRITER HTTP AVVIATO")
			let handle: NIOFileHandle
			do {
				handle = try NIOFileHandle(path: url.path)
				self?._setPayloadPhase("FILE IPA APERTO")
			} catch {
				self?._setPayloadPhase("ERRORE APERTURA IPA")
				writer.write(.error(error), promise: nil)
				self?._finishPayload(.failure(error))
				return
			}

			let fileIO = NonBlockingFileIO(threadPool: req.application.threadPool)
			let finished = fileIO.readChunked(
				fileHandle: handle,
				fromOffset: offset,
				byteCount: responseBytes,
				chunkSize: 128 * 1024,
				allocator: req.byteBufferAllocator,
				eventLoop: req.eventLoop
			) { [weak self] chunk in
				let byteCount = chunk.readableBytes
				DispatchQueue.main.async {
					self?.payloadBytesRead += Int64(byteCount)
					self?.payloadPhase = "BLOCCO LETTO, SCRITTURA IN CORSO"
					self?.payloadLastActivity = Date()
				}
				return writer.write(.buffer(chunk)).map { [weak self] in
					tracker.record(byteCount)
					self?._setPayloadPhase("SCRITTURA HTTP COMPLETATA")
				}
			}

			finished.whenComplete { result in
				try? handle.close()
				switch result {
				case .success:
					tracker.finish()
					self?._setPayloadPhase("FINE LETTURA, CHIUSURA STREAM")
					writer.write(.end, promise: nil)
				case .failure(let error):
					self?._setPayloadPhase("ERRORE LETTURA/SCRITTURA: \(error.localizedDescription)")
					writer.write(.error(error), promise: nil)
				}
				self?._finishPayload(result)
			}
		}, count: responseBytes, byteBufferAllocator: req.byteBufferAllocator)
		return response
	}

	private func _finishPayload(_ result: Result<Void, Error>) {
		switch result {
		case .success:
			_updateStatus(.installing)
		case .failure(let error):
			_updateStatus(.broken(error))
		}
	}
		
	func getServerMethod() -> Int {
		UserDefaults.standard.integer(forKey: "Feather.serverMethod")
	}
	
	func getIPFix() -> Bool {
		UserDefaults.standard.bool(forKey: "Feather.ipFix")
	}
}

/// Throttles observable progress updates to about five per second, keeping
/// the per-chunk transfer path lightweight and thread-safe.
private final class PayloadProgressTracker: @unchecked Sendable {
	private let lock = NSLock()
	private let totalBytes: Int64
	private let onUpdate: (Int64, Double) -> Void
	private var sent: Int64
	private var lastSnapshotBytes: Int64
	private var lastSnapshotTime = ProcessInfo.processInfo.systemUptime
	private var lastPublishedTime = ProcessInfo.processInfo.systemUptime

	init(startOffset: Int64, totalBytes: Int64, onUpdate: @escaping (Int64, Double) -> Void) {
		self.sent = startOffset
		self.lastSnapshotBytes = startOffset
		self.totalBytes = totalBytes
		self.onUpdate = onUpdate
	}

	func record(_ byteCount: Int) {
		lock.lock()
		sent += Int64(byteCount)
		let now = ProcessInfo.processInfo.systemUptime
		guard now - lastPublishedTime >= 0.2 || sent >= totalBytes else {
			lock.unlock()
			return
		}
		let seconds = max(now - lastSnapshotTime, 0.001)
		let speed = Double(sent - lastSnapshotBytes) / seconds
		let current = sent
		lastSnapshotBytes = sent
		lastSnapshotTime = now
		lastPublishedTime = now
		lock.unlock()
		onUpdate(current, speed)
	}

	func finish() {
		lock.lock()
		let current = sent
		lock.unlock()
		onUpdate(current, 0)
	}
}
