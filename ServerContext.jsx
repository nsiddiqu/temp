/**
 * ServerContext — auto-connects to the first server in the registry
 * so the dashboard loads immediately on startup without user interaction.
 */
import { createContext, useContext, useState, useEffect, useCallback } from 'react'
import { fetchServers, connectServer } from '../services/api'

const ServerContext = createContext(null)

export function ServerProvider({ children }) {
  const [servers, setServers]           = useState([])
  const [selectedId, setSelectedId]     = useState(null)
  const [loadingServers, setLoading]    = useState(true)
  const [connectError, setConnectError] = useState(null)

  const loadServers = useCallback(async () => {
    setLoading(true)
    try {
      const list = await fetchServers()
      setServers(list)

      // Auto-connect to first server if nothing selected yet
      if (list.length > 0 && selectedId === null) {
        const first = list[0]
        try {
          await connectServer(first.server_id)
          setSelectedId(first.server_id)
        } catch (e) {
          // Connect failed — still set it so the UI shows the server
          // and displays the error; don't leave selectedId as null
          setSelectedId(first.server_id)
          setConnectError(e.message)
        }
      }
    } catch (e) {
      console.error('Failed to load server registry:', e)
    } finally {
      setLoading(false)
    }
  }, [selectedId])

  useEffect(() => { loadServers() }, [])  // eslint-disable-line

  const selectServer = useCallback(async (serverId) => {
    setConnectError(null)
    try {
      await connectServer(serverId)
      setSelectedId(serverId)
    } catch (e) {
      setConnectError(e.message)
    }
  }, [])

  const currentServer = servers.find(s => s.server_id === selectedId) ?? null

  return (
    <ServerContext.Provider value={{
      servers,
      selectedId,
      currentServer,
      loadingServers,
      connectError,
      selectServer,
      reloadServers: loadServers,
    }}>
      {children}
    </ServerContext.Provider>
  )
}

export function useServer() {
  const ctx = useContext(ServerContext)
  if (!ctx) throw new Error('useServer must be used inside <ServerProvider>')
  return ctx
}
