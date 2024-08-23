import NIOCore
import NIOConcurrencyHelpers

/// An async sequence of ``PostgresRow``s.
///
/// - Note: This is a struct to allow us to move to a move only type easily once they become available.
public struct PostgresRowSequence: AsyncSequence, Sendable {
    public typealias Element = PostgresRow

    typealias BackingSequence = NIOThrowingAsyncSequenceProducer<DataRow, Error, AdaptiveRowBuffer, PSQLRowStream>

    private let backing: BackingSequence
    private let rowStream: PSQLRowStream
    var lookupTable: [String: Int] {
        self.rowStream.lookupTable
    }
    var columns: [RowDescription.Column] {
        self.rowStream.rowDescription
    }

    init(_ backing: BackingSequence, rowStream: PSQLRowStream) {
        self.backing = backing
        self.rowStream = rowStream
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            backing: self.backing.makeAsyncIterator(),
            lookupTable: self.lookupTable,
            columns: self.columns
        )
    }
}

extension PostgresRowSequence {
    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = PostgresRow

        let backing: BackingSequence.AsyncIterator

        let lookupTable: [String: Int]
        let columns: [RowDescription.Column]

        init(backing: BackingSequence.AsyncIterator, lookupTable: [String: Int], columns: [RowDescription.Column]) {
            self.backing = backing
            self.lookupTable = lookupTable
            self.columns = columns
        }

        public mutating func next() async throws -> PostgresRow? {
            if let dataRow = try await self.backing.next() {
                return PostgresRow(
                    data: dataRow,
                    lookupTable: self.lookupTable,
                    columns: self.columns
                )
            }
            return nil
        }
    }
}

@available(*, unavailable)
extension PostgresRowSequence.AsyncIterator: Sendable {}

extension PostgresRowSequence {
    public func collect() async throws -> [PostgresRow] {
        var result = [PostgresRow]()
        for try await row in self {
            result.append(row)
        }
        return result
    }

    public func collectWithMetadata() async throws -> (metadata: PostgresQueryMetadata, rows: [PostgresRow]) {
        let rows = try await self.collect()
        guard let metadata = PostgresQueryMetadata(string: self.rowStream.commandTag) else {
            throw PSQLError.invalidCommandTag(self.rowStream.commandTag)
        }
        return (metadata, rows)
    }
}

struct AdaptiveRowBuffer: NIOAsyncSequenceProducerBackPressureStrategy {
    static let defaultBufferTarget = 256
    static let defaultBufferMinimum = 1
    static let defaultBufferMaximum = 16384

    let minimum: Int
    let maximum: Int

    private var target: Int
    private var canShrink: Bool = false

    init(minimum: Int, maximum: Int, target: Int) {
        precondition(minimum <= target && target <= maximum)
        self.minimum = minimum
        self.maximum = maximum
        self.target = target
    }

    init() {
        self.init(
            minimum: Self.defaultBufferMinimum,
            maximum: Self.defaultBufferMaximum,
            target: Self.defaultBufferTarget
        )
    }

    mutating func didYield(bufferDepth: Int) -> Bool {
        if bufferDepth > self.target, self.canShrink, self.target > self.minimum {
            self.target &>>= 1
        }
        self.canShrink = true

        return false // bufferDepth < self.target
    }

    mutating func didConsume(bufferDepth: Int) -> Bool {
        // If the buffer is drained now, we should double our target size.
        if bufferDepth == 0, self.target < self.maximum {
            self.target = self.target * 2
            self.canShrink = false
        }

        return bufferDepth < self.target
    }
}
